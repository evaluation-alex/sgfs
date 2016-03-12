# Scatter-gather filesystem

SGFS is a [union mount](https://en.wikipedia.org/wiki/Union_mount) filesystem implemented using [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace).
It takes multiple source filesystems, and presents a unified view of them in the destination mount point.
Instead of overlaying source filesystems on top of each other (like with overlayfs, unionfs, aufs),
where typically only the top layer is writable,
SGFS treats source filesystems equally and allows them all to be written to.
The assumption is made that a given filename exists only in one of the source directories.
When creating new files, it tries to choose the source filesystem which already has other files in the same directory,
otherwise the source with the largest amount of free space will be chosen.

## Example usage

    sgfs destination source1 source2 [sourceN...]
