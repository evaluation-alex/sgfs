#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdint.h>
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

static uint32_t crc_table[256];

static void make_crc_table(void) {
	unsigned long c;
	int n, k;

	for (n = 0; n < 256; n++) {
		c = (unsigned long) n;
		for (k = 0; k < 8; k++) {
			if (c & 1)
				c = 0xedb88320L ^ (c >> 1);
			else
				c = c >> 1;
			crc_table[n] = c;
		}
	}
}

static uint32_t update_crc(uint32_t crc, const unsigned char *buf, int len) {
	static bool crc_table_computed = false;

	if (!crc_table_computed) {
		make_crc_table();
		crc_table_computed = true;
	}

	crc ^= 0xffffffffL;

	for (int n = 0; n < len; n++)
		crc = crc_table[(crc ^ buf[n]) & 0xff] ^ (crc >> 8);

	return crc ^ 0xffffffffL;
}

static uint32_t crc(const char *buf, int len) {
	return update_crc(0L, (const unsigned char *)buf, len);
}

static int sgfs_statfs(const char *path, struct statvfs *stbuf) {
	struct statvfs st;
	uint64_t total = 0;
	uint64_t free = 0;
	uint64_t avail = 0;

	memset(stbuf, 0, sizeof stbuf);
	stbuf->f_flag = ~0UL;

	for(int i = 0; i < unders; i++) {
		if(fstatvfs(under_fd[i], &st))
			return -EIO;

		if(stbuf->f_bsize < st.f_bsize)
			stbuf->f_bsize = st.f_bsize;
		if(stbuf->f_frsize < st.f_frsize)
			stbuf->f_frsize = st.f_frsize;
		if(!stbuf->f_namemax || stbuf->f_namemax > st.f_namemax)
			stbuf->f_namemax = st.f_namemax;

		total += st.f_blocks * st.f_frsize;
		free += st.f_bfree * st.f_frsize;
		avail += st.f_bavail * st.f_frsize;

		stbuf->f_files += st.f_files;
		stbuf->f_ffree += st.f_ffree;
		stbuf->f_favail += st.f_favail;

		stbuf->f_fsid ^= st.f_fsid;
		stbuf->f_flag &= st.f_flag;
	}

	stbuf->f_blocks = total / stbuf->f_frsize;
	stbuf->f_bfree = free / stbuf->f_frsize;
	stbuf->f_bavail = avail / stbuf->f_frsize;

	return 0;
}

static int fix_tree(int uf, const char *path, int ut) {
	// Find under with destination path if necessary

	char basename[strlen(path)];

	strcpy(basename, path + 1);
	char *slash = strrchr(basename, '/');
	if(!slash)
		return 0; // Bail out early, every underlay has the root dir!
	else
		*slash = 0;

	struct stat stdir;

	if(uf < 0) {
		// First, check if target already has the full directory tree

		int res = fstatat(under_fd[ut], basename, &stdir, AT_SYMLINK_NOFOLLOW);
		if(!res && S_ISDIR(stdir.st_mode))
			return 0;

		// Check all other unders

		for(int i = 0; i < unders; i++) {
			if(i == ut)
				continue;

			res = fstatat(under_fd[i], basename, &stdir, AT_SYMLINK_NOFOLLOW);
			if(!res && S_ISDIR(stdir.st_mode)) {
				uf = i;
				break;
			}
		}
		if(uf < 0)
			return -ENOENT;
	}

	// Copy directory tree

	char *split = basename;

	do {
		split = strchr(split, '/');
		if(split)
			*split = 0;
		int res = fstatat(under_fd[uf], basename, &stdir, AT_SYMLINK_NOFOLLOW);
		if(res || !S_ISDIR(stdir.st_mode))
			return -EIO;
		res = mkdirat(under_fd[ut], basename, stdir.st_mode);
		if(res && errno != EEXIST)
			return -errno;
		if(split)
			*split++ = '/';
	} while(split);

	return 0;
}

static int get_best_under(const char *path, int mode) {
	bool hasdir[unders];
	uint64_t ffree[unders];
	char basename[strlen(path)];
	int u = -1;

	if(!path[1])
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
		if(fstatvfs(under_fd[i], &st))
			return -EIO;

		ffree[i] = (uint64_t)st.f_bsize * st.f_bfree;

		int res = fstatat(under_fd[i], basename, &stdir, AT_SYMLINK_NOFOLLOW);
		if(!res && S_ISDIR(stdir.st_mode)) {
			hasdir[i] = true;
			if(u < 0)
				u = i;
		}
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

	// If the selected underlay does not have the desired directory,
	// we have to create it and possibly all its parents.

	if(!hasdir[j]) {
		if(u < 0)
			return -EIO; // At least one underlay should have the directory we want to make!

		char *split = basename;

		do {
			split = strchr(split, '/');
			if(split)
				*split = 0;
			int res = fstatat(under_fd[u], basename, &stdir, AT_SYMLINK_NOFOLLOW);
			if(res || !S_ISDIR(stdir.st_mode))
				return -EIO;
			res = mkdirat(under_fd[j], basename, stdir.st_mode);
			if(res && errno != EEXIST)
				return -errno;
			if(split)
				*split++ = '/';
		} while(split);
	}

	return j;
}

static char creatpath[PATH_MAX] = "";
static int creatfd = -1;
static int creatunder = -1;

static int sgfs_mknod(const char *path, mode_t mode, dev_t rdev) {
	int res = get_best_under(path, mode);
	if(res < 0)
		return res;
	if(S_ISREG(mode)) {
		int fd = openat(under_fd[res], path + 1, O_RDWR | O_CREAT | O_EXCL, mode & 007777);
		if(fd < 0)
			return -errno;
		if(creatfd >= 0)
			close(creatfd);
		creatfd = fd;
		creatunder = res;
		strncpy(creatpath, path, sizeof creatpath);
		return 0;
	}

	res = mknodat(under_fd[res], path + 1, mode, rdev);
	return res ? -errno : 0;
}

static int sgfs_mkdir(const char *path, mode_t mode) {
	int res = get_best_under(path, mode);
	if(res < 0)
		return res;
	res = mkdirat(under_fd[res], path + 1, mode);
	return res ? -errno : 0;
}

static int sgfs_symlink(const char *from, const char *to) {
	if(!to[1])
		return -EINVAL;

	// First check whether the destination file already exists in any of the underlays
	for(int i = 0; i < unders; i++) {
		if(!faccessat(under_fd[i], to + 1, F_OK, AT_SYMLINK_NOFOLLOW))
			return -EEXIST;
	}

	int res = get_best_under(to, 0);
	if(res < 0)
		return res;
	res = symlinkat(from, under_fd[res], to + 1);
	return res ? -errno : 0;
}

static int sgfs_link(const char *from, const char *to) {
	if(!from[1] || !to[1])
		return -EINVAL;

	// First check whether the destination file already exists in any of the underlays
	for(int i = 0; i < unders; i++) {
		if(!faccessat(under_fd[i], to + 1, F_OK, AT_SYMLINK_NOFOLLOW))
			return -EEXIST;
	}

	// TODO: when linking to a directory that does not exist on the underlay
	// of the original file, we need to create that tree.

	for(int i = 0; i < unders; i++) {
		if(faccessat(under_fd[i], from + 1, F_OK, AT_SYMLINK_NOFOLLOW))
			continue;
		int res = fix_tree(-1, to, i);
		if(res)
			return res;
		res = linkat(under_fd[i], from + 1, under_fd[i], to + 1, 0);
		if(!res)
			return 0;
		if(errno != ENOENT)
			return -errno;
	}

	return -ENOENT;
}

static int sgfs_access(const char *path, int mode) {
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
	if(!path[1])
		return -EINVAL;

	for(int i = 0; i < unders; i++) {
		int res = unlinkat(under_fd[i], path + 1, 0);
		if(res && errno == ENOENT)
			continue;
		return res ? -errno : 0;
	}

	return -ENOENT;
}

static int sgfs_rmdir(const char *path) {
	bool found = false;

	if(!path[1])
		return -EINVAL;

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

static int sgfs_truncate(const char *path, off_t size) {
	bool found = false;

	if(!path[1])
		return -EINVAL;

	for(int i = 0; i < unders; i++) {
		if(fchdir(under_fd[i]))
			return -EIO;

		int res = truncate(path + 1, size);
		if(res && errno == ENOENT)
			continue;
		if(res)
			return -errno;
		found = true;
	}

	return found ? 0 : -ENOENT;
}

static int sgfs_rename(const char *path, const char *to) {
	if(!path[1] || !to[1])
		return -EINVAL;

	// Find file/directory to rename

	struct stat st;
	int uf = -1;

	for(int i = 0; i < unders; i++) {
		int res = fstatat(under_fd[i], path + 1, &st, AT_SYMLINK_NOFOLLOW);
		if(res && errno == ENOENT)
			continue;
		if(res)
			return -errno;
		uf = i;
		break;
	}

	if(uf < 0)
		return -ENOENT;

	if(S_ISDIR(st.st_mode)) {
		// Move directory in all unders
		for(int i = 0; i < unders; i++) {
			int res = fstatat(under_fd[i], path + 1, &st, AT_SYMLINK_NOFOLLOW);
			if(res && errno == ENOENT)
				continue;
			if(res)
				return -errno;
			// Check that it is also a directory on the other unders
			if(!S_ISDIR(st.st_mode))
				return -EIO;
			res = fix_tree(-1, to, i);
			if(res)
				return res;

			res = renameat(under_fd[i], path + 1, under_fd[i], to + 1);
			if(res)
				return -errno;
		}
	} else {
		// Move file in its own under
		int res = fix_tree(-1, to, uf);
		if(res)
			return res;
		res = renameat(under_fd[uf], path + 1, under_fd[uf], to + 1);
		if(res)
			return -errno;

		// Check whether the destination file exists in another under, and remove if so
		for(int i = 0; i < unders; i++) {
			if(i == uf)
				continue;
			if(!faccessat(under_fd[i], to + 1, F_OK, AT_SYMLINK_NOFOLLOW))
				if(unlinkat(under_fd[i], to + 1, 0))
					return -errno;
		}
	}

	return 0;
}

static int sgfs_chmod(const char *path, mode_t mode) {
	bool found = false;

	if(!path[1])
		return -EINVAL;

	for(int i = 0; i < unders; i++) {
		int res = fchmodat(under_fd[i], path + 1, mode, 0); // Should have AT_SYMLINK_NOFOLLOW but is not implemented?
		if(res && errno == ENOENT)
			continue;
		if(res)
			return -errno;
		found = true;
	}

	return found ? 0 : -ENOENT;
}

static int sgfs_chown(const char *path, uid_t uid, gid_t gid) {
	bool found = false;

	if(!path[1])
		return -EINVAL;

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
	bool found = false;

	if(!path[1])
		return -EINVAL;

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
	int i;
	int res = -1;
	errno = ENOENT;

	if(creatfd >= 0 && !strcmp(creatpath, path)) {
		struct sgfs_file *f = malloc(sizeof *f);
		f->under = creatunder;
		f->rfd = creatfd;
		f->wfd = creatfd;
		fi->fh = (intptr_t)f;
		creatfd = -1;
		return 0;
	} else if(creatfd >= 0) {
		close(creatfd);
		creatfd = -1;
	}

	for(i = 0; i < unders; i++) {
		res = openat(under_fd[i], path[1] ? path + 1 : ".", O_RDONLY);
		if(res >= 0 || errno != ENOENT)
			break;
	}

	if(res >= 0) {
		struct sgfs_file *f = malloc(sizeof *f);
		f->under = i;
		f->rfd = res;
		f->wfd = -1;
		fi->fh = (intptr_t)f;
		return 0;
	} else {
		return -errno;
	}
}

static int sgfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
	struct sgfs_file *f = (struct sgfs_file *)fi->fh;
	int res = pread(f->rfd, buf, size, offset);
	return res < 0 ? -errno : res;
}

static int sgfs_fsync(const char *path, int idatasync, struct fuse_file_info *fi) {
	struct sgfs_file *f = (struct sgfs_file *)fi->fh;
	int res = idatasync ? fdatasync(f->rfd) : fsync(f->rfd);
	return res < 0 ? -errno : res;
}

static int sgfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
	struct sgfs_file *f = (struct sgfs_file *)fi->fh;
	if(f->wfd < 0) {
		f->wfd = openat(under_fd[f->under], path[1] ? path + 1 : ".", O_WRONLY);
		if(f->wfd < 0)
			return -errno;
	}

	int res = pwrite(f->wfd, buf, size, offset);
	return res < 0 ? -errno : res;
}

static int sgfs_release(const char *path, struct fuse_file_info *fi) {
	struct sgfs_file *f = (struct sgfs_file *)fi->fh;
	if(f->wfd && f->wfd != f->rfd)
		close(f->wfd);
	return close(f->rfd);
}

static int sgfs_getattr(const char *path, struct stat *stbuf) {
	int res = -1;
	errno = ENOENT;

	for(int i = 0; i < unders; i++) {
        	res = fstatat(under_fd[i], path[1] ? path + 1 : ".", stbuf, AT_SYMLINK_NOFOLLOW);
		if(!res || errno != ENOENT)
			break;
	}

	return res ? -errno : 0;
}

static int sgfs_opendir(const char *path, struct fuse_file_info *fi) {
	return 0;
}

static int sgfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
	bool found = 0;

	if(offset) {
		fprintf(stderr, "Cannot handle offsets!\n");
		return -EINVAL;
	}

	char hbm[8192] = ""; // 64k entry hash bitmap for duplicate detection

	for(int i = 0; i < unders; i++) {
		if(fchdir(under_fd[i]))
			return -EIO;

		DIR *dir = opendir(path[1] ? path + 1 : ".");

		if(!dir && errno == ENOENT)
			continue;
		else
			found = true;

		if(!dir)
			return -errno;

		struct dirent *ent;

		while((ent = readdir(dir))) {
			// Do duplicate detection for directories
			if(ent->d_type == DT_UNKNOWN || ent->d_type == DT_DIR) {
				uint16_t hash = crc(ent->d_name, strlen(ent->d_name));
				uint8_t mask = 1 << (hash & 0x7);
				hash >>= 3;

				if(hbm[hash] & mask) {
					bool found = false;

					for(int j = 0; j < i; j++) {
						char entpath[PATH_MAX];
						strncpy(entpath, path[1] ? path + 1 : ".", sizeof entpath - 1);
						strncat(entpath, "/", sizeof entpath - 1);
						strncat(entpath, ent->d_name, sizeof entpath - 1);
						entpath[PATH_MAX - 1] = 0;
						if(!faccessat(under_fd[i], entpath, F_OK, AT_SYMLINK_NOFOLLOW)) {
							found = true;
							break;
						}
					}

					if(found)
						continue;
				} else {
					hbm[hash] |= mask;
				}
			}

			struct stat st;
			memset(&st, 0, sizeof st);
			st.st_ino = ent->d_ino;
			st.st_mode = ent->d_type << 12;

			if(filler(buf, ent->d_name, &st, 0))
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
	.truncate = sgfs_truncate,
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

	for(int i = 0; i < unders; i++) {
		under_fd[i] = open(under_name[i], O_RDONLY | O_DIRECTORY);
		if(under_fd[i] < 0) {
			fprintf(stderr, "Could not open %s: %s\n", under_name[i], strerror(errno));
			exit(1);
		}
	}

	fuse_main(args.argc, args.argv, &sgfs_oper, NULL);
}
