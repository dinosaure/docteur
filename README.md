# Docteur - the simple way to load your Git repository into your unikernel

`docteur` is a little program which wants to provide an easy way to integrate
a "file-system" into an _unikernel_. `docteur` provides a simple binary which
make an image disk from a Git repository. Then, the user is able to "plug" this
image into an _unikernel_ as a read-only snapshot.

## Example

The distribution comes with a simple unikernel which show the given file from
the given image disk. The example requires KVM.

```sh
$ git clone https://github.com/dinosaure/docteur
$ cd docteur
$ opam pin add -y .
$ cd unikernel
$ docteur.make https://github.com/dinosaure/docteur refs/heads/main disk.img
$ mirage configure -t hvt --disk docteur
$ mirage build
$ solo5-hvt --block:docteur=disk.img simple.hvt --filename /README.md
...
```

## What such library wants to solve?

MirageOS does not have a proper file-system as we believe. However, we agree
that it should be better to have, at least, a read-only file-system in 
MirageOS. And we already have with `ocaml-git` and `irmin`. However, these
libraries (to be able to read **and** write) use the memory instead of a
hard-disk to store your file-system.

`docteur` is an _half_-solution and propose to save your versionned
file-system into an image disk. Then, something else exists on the
MirageOS side to manipulate this image disk and get files and directories of
what you saved.

The advantage is the usage of the memory by the unikernel. We don't really
need to grow the memory according to what we have into your file-system. The
second advantage is the inherent integrity check of your file-system by a
specific hash (currently, we use SHA1 due to Git but we can easily upgrade
to something else). Finally, the other advantage is the compatibility with Git
when we just re-use the PACK file as the representation of your file-system

However, the boot can be slow! Indeed, the first step of the unikernel is to
check the integrity of your file-system and more the file-system is big,
longer is such process. The other disadvantage is the read-only capability.
In fact, we can not write into the image disk. But eh, it's the goal!
