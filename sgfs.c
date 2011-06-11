#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

char **under_name;
const char *mountpoint;
int *under_fd;
int unders;

enum {
	KEY_HELP,
	KEY_VERSION,
};

static const uint64_t MINFREE = 1024*1024*128; // 128 MB

struct sgfs_file {
	int rfd;
	int wfd;
	int under;
};

static int sgfs_statfs(const char *path, struct statvfs *stbuf) {
	fprintf(stderr, "statfs(%s)\n", path);

	struct statvfs st;

	memset(stbuf, 0, sizeof stbuf);

	for(int i = 0; i < unders; i++) {
		if(fstatvfs(under_fd[i], &st)) {
			fprintf(stderr, "fstatvfs(%s) failed: %s\n", under_name[i], strerror(errno));
			continue;
		}

		stbuf->f_bsize += st.f_bsize;
		stbuf->f_frsize += st.f_frsize;
		stbuf->f_blocks += st.f_blocks;
		stbuf->f_bfree += st.f_bfree;
		stbuf->f_bavail += st.f_bavail;
		stbuf->f_files += st.f_files;
		stbuf->f_ffree += st.f_ffree;
		stbuf->f_favail += st.f_favail;
		//stbuf->f_fsid += st.f_fsid;
		//stbuf->f_flag += st.f_flag;
		stbuf->f_namemax += st.f_namemax;
	}

	stbuf->f_bsize /= unders;
	stbuf->f_frsize /= unders;
	stbuf->f_namemax /= unders;

	return 0;
}

static int get_best_under(const char *path, int mode) {
	bool hasdir[unders];
	uint64_t ffree[unders];
	char basename[strlen(path)];
	mode_t dirmode = 0;

	if(!*path)
		return -EINVAL;

	strcpy(basename, path + 1);
	char *slash = strrchr(basename, '/');
	if(!slash)
		strcpy(basename, ".");
	else
		*slash = 0;

	// Check which filesystem has a directory containing new node
	// Check if size and attributes allow new stuff on FS
	// If fail, find suitable one

	memset(hasdir, 0, unders * sizeof *hasdir);
	memset(ffree, 0, unders * sizeof *ffree);

	struct statvfs st;
	struct stat stdir;

	for(int i = 0; i < unders; i++) {
		if(fstatvfs(under_fd[i], &st)) {
			fprintf(stderr, "fstatvfs(%s) failed: %s\n", under_name[i], strerror(errno));
			continue;
		}

		ffree[i] = (uint64_t)st.f_bsize * st.f_bfree;

		int res = fstatat(under_fd[i], basename, &stdir, AT_SYMLINK_NOFOLLOW);
		if(!res && S_ISDIR(stdir.st_mode)) {
			hasdir[i] = true;
			if(!dirmode)
				dirmode = stdir.st_mode;
		}

		fprintf(stderr, "%d %s %d %ld\n", i, under_name[i], hasdir[i], (long)ffree[i]);
	}

	for(int i = 0; i < unders; i++) {
		if(!hasdir[i])
			continue;

		if(ffree[i] < MINFREE)
			continue;

		return i;
	}

	uint64_t largest = ffree[0];
	int j = 0;

	for(int i = 1; i < unders; i++) {
		if(ffree[i] > largest) {
			largest = ffree[i];
			j = i;
		}
	}

	if(!hasdir[j]) {
		if(!dirmode)
			dirmode = S_IRWXU;
		int res = mkdirat(under_fd[j], basename, mode);
		if(res)
			return -errno;
	}

	return j;
}

static int sgfs_mknod(const char *path, mode_t mode, dev_t rdev) {
	fprintf(stderr, "mknod(%s, %d, %d)\n", path, (int)mode, (int)rdev);
	int res = get_best_under(path, mode);
	if(res < 0)
		return res;
	res = mknodat(under_fd[res], path + 1, mode, rdev);
	return res ? -errno : 0;
}

static int sgfs_mkdir(const char *path, mode_t mode) {
	fprintf(stderr, "mkdir(%s, %d)n", path, (int)mode);
	int res = get_best_under(path, mode);
	if(res < 0)
		return res;
	res = mkdirat(under_fd[res], path + 1, mode);
	return res ? -errno : 0;
}

static int sgfs_symlink(const char *path, const char *to) {
	fprintf(stderr, "symlink(%s, %s)\n", path, to);
	int res = get_best_under(path, 0);
	if(res < 0)
		return res;
	res = symlinkat(path, under_fd[res], to);
	return res ? -errno : 0;
}

static int sgfs_link(const char *path, const char *to) {
	fprintf(stderr, "link(%s, %s)\n", path, to);
	int res = get_best_under(path, 0);
	if(res < 0)
		return res;
	res = linkat(under_fd[res], path, under_fd[res], to, 0);
	return res ? -errno : 0;
}

static int sgfs_access(const char *path, int mode) {
	fprintf(stderr, "access(%s, %d)\n", path, mode);

	for(int i = 0; i < unders; i++) {
		int res = faccessat(under_fd[i], path[1] ? path + 1 : ".", mode, AT_SYMLINK_NOFOLLOW);
		if(!res)
			return 0;
		if(errno != ENOENT)
			return -errno;
	}

	return -ENOENT;
}

static int sgfs_readlink(const char *path, char *buf, size_t size) {
	fprintf(stderr, "readlink(%s, %p, %zd)\n", path, buf, size);

	for(int i = 0; i < unders; i++) {
		int res = readlinkat(under_fd[i], path[1] ? path + 1 : ".", buf, size - 1);
		if(res < 0) {
			if(errno != ENOENT)
				return -errno;
			continue;
		}

		buf[res] = 0;
		return 0;
	}

	return -ENOENT;
}

static int sgfs_unlink(const char *path) {
	fprintf(stderr, "unlink(%s)\n", path);

	if(!*path)
		return -EIO;

	for(int i = 0; i < unders; i++) {
		int res = unlinkat(under_fd[i], path + 1, 0);
		if(res && errno == ENOENT)
			continue;
		return res ? -errno : 0;
	}

	return -ENOENT;
}

static int sgfs_rmdir(const char *path) {
	fprintf(stderr, "rmdir(%s)\n", path);

	bool found = false;

	if(!*path)
		return -EIO;

	for(int i = 0; i < unders; i++) {
		int res = unlinkat(under_fd[i], path + 1, AT_REMOVEDIR);
		if(res && errno == ENOENT)
			continue;
		if(res)
			return -errno;
		found = true;
	}

	return found ? 0 : -ENOENT;
}

static int sgfs_rename(const char *path, const char *to) {
	fprintf(stderr, "rename(%s, %s)\n", path, to);

	bool found = false;

	if(!*path || !*to)
		return -EIO;

	for(int i = 0; i < unders; i++) {
		int res = renameat(under_fd[i], path + 1, under_fd[i], to + 1);
		if(res && errno == ENOENT)
			continue;
		if(res)
			return -errno;
		found = true;
	}

	return found ? 0 : -ENOENT;
}

static int sgfs_chmod(const char *path, mode_t mode) {
	fprintf(stderr, "chmod(%s, %d)\n", path, (int)mode);

	bool found = false;

	if(!*path)
		return -EIO;

	for(int i = 0; i < unders; i++) {
		int res = fchmodat(under_fd[i], path + 1, mode, AT_SYMLINK_NOFOLLOW);
		if(res && errno == ENOENT)
			continue;
		if(res)
			return -errno;
		found = true;
	}

	return found ? 0 : -ENOENT;
}

static int sgfs_chown(const char *path, uid_t uid, gid_t gid) {
	fprintf(stderr, "chown(%s, %d, %d)\n", path, (int)uid, (int)gid);

	bool found = false;

	if(!*path)
		return -EIO;

	for(int i = 0; i < unders; i++) {
		int res = fchownat(under_fd[i], path + 1, uid, gid, AT_SYMLINK_NOFOLLOW);
		if(res && errno == ENOENT)
			continue;
		if(res)
			return -errno;
		found = true;
	}

	return found ? 0 : -ENOENT;
}

static int sgfs_utimens(const char *path, const struct timespec ts[2]) {
	fprintf(stderr, "utimens(%s)\n", path);

	bool found = false;

	if(!*path)
		return -EIO;

	for(int i = 0; i < unders; i++) {
		int res = utimensat(under_fd[i], path + 1, ts, AT_SYMLINK_NOFOLLOW);
		if(res && errno == ENOENT)
			continue;
		if(res)
			return -errno;
		found = true;
	}

	return found ? 0 : -ENOENT;
}


static int sgfs_open(const char *path, struct fuse_file_info *fi) {
	fprintf(stderr, "open(%s)\n", path);

	int i;
	int res = -1;
	errno = ENOENT;

	for(i = 0; i < unders; i++) {
		res = openat(under_fd[i], path[1] ? path + 1 : ".", O_RDONLY);
		if(res >= 0 || errno != ENOENT)
			break;
	}

	fprintf(stderr, "%d %s\n", res, strerror(errno));

	if(res >= 0) {
		struct sgfs_file *f = malloc(sizeof *f);
		f->under = i;
		f->rfd = res;
		f->wfd = -1;
		fi->fh = f;
		return 0;
	} else {
		return -errno;
	}
}

static int sgfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
	struct sgfs_file *f = fi->fh;
	fprintf(stderr, "read(%s %d, %p, %zd, %zd) = ", path, f->rfd, buf, size, (size_t)offset);
	int res = pread(f->rfd, buf, size, offset);
	fprintf(stderr, "%d (%s)\n", res, strerror(errno));
	return res < 0 ? -errno : res;
}

static int sgfs_fsync(const char *path, int idatasync, struct fuse_file_info *fi) {
	struct sgfs_file *f = fi->fh;
	fprintf(stderr, "fsync(%s %d %d) = ", path, f->rfd, idatasync);
	int res = idatasync ? fdatasync(f->rfd) : fsync(f->rfd);
	fprintf(stderr, "%d (%s)\n", res, strerror(errno));
	return res < 0 ? -errno : res;
}

static int sgfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
	struct sgfs_file *f = fi->fh;
	fprintf(stderr, "write(%s %d %d/%d, %p, %zd, %zd) = ", path, f->rfd, f->wfd, f->under, buf, size, (size_t)offset);
	if(f->wfd < 0) {
		f->wfd = openat(under_fd[f->under], path[1] ? path + 1 : ".", O_WRONLY);
		fprintf(stderr, "open(%s, O_WRONLY) = %d\n", path[1] ? path + 1 : ".", f->wfd);
		if(f->wfd < 0)
			return -errno;
	}

	int res = pwrite(f->wfd, buf, size, offset);
	fprintf(stderr, "%d (%s)\n", res, strerror(errno));
	return res < 0 ? -errno : res;
}

static int sgfs_release(const char *path, struct fuse_file_info *fi) {
	struct sgfs_file *f = fi->fh;
	if(f->wfd)
		close(f->wfd);
	return close(f->rfd);
}

static int sgfs_getattr(const char *path, struct stat *stbuf) {
	fprintf(stderr, "getattr(%s)\n", path);

	int res = -1;
	errno = ENOENT;

	for(int i = 0; i < unders; i++) {
        	res = fstatat(under_fd[i], path[1] ? path + 1 : ".", stbuf, AT_SYMLINK_NOFOLLOW);
		if(!res || errno != ENOENT)
			break;
	}

	fprintf(stderr, "res = %d\n", res);
	return res ? -errno : 0;
}

static int sgfs_opendir(const char *path, struct fuse_file_info *fi) {
	return 0;
}

static int sgfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
	fprintf(stderr, "readdir(%s, %ld)\n", path, (long)offset);

	bool found = 0;

	if(offset) {
		fprintf(stderr, "Cannot handle offsets!\n");
		return 0;
	}

	bool dot = false;
	bool dotdot = false;

	for(int i = 0; i < unders; i++) {
		if(fchdir(under_fd[i])) {
			fprintf(stderr, "fchdir(%s) failed: %s\n", under_name[i], strerror(errno));
			continue;
		}

		DIR *dir = opendir(path[1] ? path + 1 : ".");
		if(!dir)
			fprintf(stderr, "opendir(%s) failed: %s\n", path + 1, strerror(errno));

		if(!dir && errno == ENOENT)
			continue;
		else
			found = true;

		if(!dir)
			return -errno;

		struct dirent *ent;

		while((ent = readdir(dir))) {
			fprintf(stderr, "%s\n", ent->d_name);
			if(!strcmp(ent->d_name, ".")) {
				if(dot)
					continue;
				else
					dot = true;
			}

			if(!strcmp(ent->d_name, "..")) {
				if(dotdot)
					continue;
				else
					dotdot = true;
			}

			struct stat st;
			memset(&st, 0, sizeof st);
			st.st_ino = ent->d_ino;
			st.st_mode = ent->d_type << 12;

			if(filler(buf, ent->d_name, &st, telldir(dir)))
				break;
		}

		closedir(dir);
	}

	if(!found)
		return -ENOENT;

	return 0;
}

static int sgfs_releasedir(const char *path, struct fuse_file_info *fi) {
	return 0;
}

static struct fuse_operations sgfs_oper = {
	.mknod = sgfs_mknod,
	.mkdir = sgfs_mkdir,
	.unlink = sgfs_unlink,
	.rmdir = sgfs_rmdir,
	.rename = sgfs_rename,
	.chmod = sgfs_chmod,
	.chown = sgfs_chown,
	.utimens = sgfs_utimens,
	.symlink = sgfs_symlink,
	.link = sgfs_link,
	.access = sgfs_access,
	.readlink = sgfs_readlink,
	.statfs = sgfs_statfs,
	.open = sgfs_open,
	.read = sgfs_read,
	.fsync = sgfs_fsync,
	.write = sgfs_write,
	.release = sgfs_release,
	.getattr = sgfs_getattr,
	.opendir = sgfs_opendir,
	.readdir = sgfs_readdir,
	.releasedir = sgfs_releasedir,
};

static int sgfs_parse_opt(void *data, const char *arg, int key, struct fuse_args *outargs) {
	switch(key) {
		case FUSE_OPT_KEY_NONOPT:
			if(!mountpoint) {
				mountpoint = strdup(arg);
				return 1;
			}
			under_name = realloc(under_name, (unders + 1) * sizeof *under_name);
			under_fd = realloc(under_fd, (unders + 1) * sizeof *under_fd);
			under_name[unders] = strdup(arg);
			unders++;
			return 0;
		case FUSE_OPT_KEY_OPT:
			return 1;
		case KEY_HELP:
			fuse_opt_add_arg(outargs, "-h");
			return 0;
		default:
			fprintf(stderr, "Unknown option %d %s\n", key, arg);
			abort();
	}

	return -1;
}

static struct fuse_opt sgfs_opts[] = {
	FUSE_OPT_KEY("-h", KEY_HELP),
	FUSE_OPT_KEY("--help", KEY_HELP),
	FUSE_OPT_END
};

int main(int argc, char *argv[]) {
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	int res = fuse_opt_parse(&args, NULL, sgfs_opts, sgfs_parse_opt);
	if(res) {
		fprintf(stderr, "Invalid arguments\n");
		return 1;
	}

	fprintf(stderr, "mountpoint = %s, unders = %d\n", mountpoint, unders);

	for(int i = 0; i < unders; i++) {
		under_fd[i] = open(under_name[i], O_RDONLY | O_DIRECTORY);
		if(under_fd[i] < 0) {
			fprintf(stderr, "Could not open %s: %s\n", under_name[i], strerror(errno));
			exit(1);
		}
	}

	fuse_main(args.argc, args.argv, &sgfs_oper, NULL);
}
