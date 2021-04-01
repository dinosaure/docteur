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
$ docteur.make https://github.com/dinosaure/docteur refs/heads/master disk.img
$ mirage configure -t hvt --disk docteur
$ mirage build
$ solo5-hvt --block:docteur=disk.img simple.hvt --filename /README.md
...
```
