# Docteur - the simple way to load your Git repository into your unikernel

`docteur` is a little program which wants to provide an easy way to integrate
a "file-system" into an _unikernel_. `docteur` provides a simple binary which
make an image disk from a Git repository. Then, the user is able to "plug" this
image into an _unikernel_ as a read-only "file-system".

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
$ make depends
$ mirage build
$ solo5-hvt --block:docteur=disk.img simple.hvt --filename /README.md
...
```

**NOTE:** For `mirage -t unix`, the disk name is the filename:
```sh
$ mirage configure -t unix --disk disk.img
$ mirage build
$ make depends
$ ./simple --filename /README.md
```

An image can be checked by `docteur with `docteur.verify`:
```sh
$ docteur.verify disk.img
commit	: 57d227d8f4808076646de35acf26dee885f2555b
author	: "Calascibetta Romain" <romain.calascibetta@gmail.com>
root	: 5886893922d57c1ff4871d9a6b7b2cfa48b9e9a6

Merge pull request #22 from dinosaure/without-c

Remove C code to be compatible with MirageOS
```

By this way, you can check the version of your snapshot and if the given
`disk.img` is well formed for a MirageOS.

Docteur is able to _save_ a remote Git repository, a local Git repository or a
simple directory:
``` sh
$ docteur.make git@github.com:dinosaure/docteur disk.img
$ docteur.make https://github.com/dinosaure/docteur disk.img
$ docteur.make https://user:password@github.com/dinosaure/docteur disk.img
$ docteur.make git://github.com/dinosaure/docteur disk.img
$ docteur.make file://$(pwd/ disk.img 
  ; assume that $(pwd) is a local Git repository
  ; $(pwd)/.git exists
$ docteur.make file://$(pwd)/ disk.img
  ; or it's a simple directory
```

**NOTE:** The last example can be less efficient (about compression) than
others because we directly use our own way to generate a PACK file (which is
less smart than `git`).

## Docteur as a file-system

MirageOS does not have a file-system at the beginning. So we must implement one
to get the idea of files and directories. Multiple designs exist and no one are
perfect for any cases.

However, `docteur` exists as one possible "file-system" for MirageOS. It's not
the only one but it deserves a special case. Indeed, you can look into
[irmin][irmin] and [ocaml-git][ocaml-git] for an other one.

Docteur provides only a read-only file-system and contents are not a part of
the _unikernel_. Only _meta-data_ are in the _unikernel_. Let me explain a bit
the format.

## The PACK file

In your Git repositories, most of your Git objects (files, directories,
commits) are stored into a [PACK file][pack-file]. It's an highly compressed
representation of your Git repository (your history, your files, etc.). Indeed,
the PACK file has 2 levels of compression:
1) a `zlib` compression for each objects
2) a compression between objects with a binary diff ([libXdiff][libXdiff])

For example, 14 Go of contents (like a documentation) can fit into a PACK file
of 280 Mo! It's mostly due to the fact that a documentation, for example, has
several files which are pretty the same. According to the second level of
the compression, we can store few objects as bases and compress the rest of
the documentation with them.

So, `docteur` uses the same format as an image disk. Then, it re-uses the
IDX file associated to the PACK file. By this way, we permit as fast access
to the content.

Finally, contents of objects (files or directories) and where they are from
their hashes into the PACK file are statically produced by `docteur.make`:
```sh
$ docteur.make <repository> [-b <refs>] <image>
$ docteur.make https://github.com/dinosaure/docteur -b refs/heads/main disk.img
```

However, the indexation of objects is done by their hashes. It's not done by
their locations in your system. Such information is calculated by the
_unikernel_ itself. At the beginning, it analyzes the PACK file and the IDX
file to reconstruct the system's layout with filenames and directory names.

So, the more files there are, the longer this operation can take - and the more
memory you use. Indeed, the system's layout is stored into memory with the
[`art`][art] data-structure. Even if such data-structure is faster and smaller
than an usual radix tree, if you take the example of a huge documentation,
the _unikernel_ needs ~650 Mo in memory.

`docteur` wants to solve 2 issues:
- How to access to a huge file-system into an unikernel
  We can from a block-device (an external ressource of the unikernel)
- How to fastly load a file
  We use a fast data-structure in-memory to get contents with [art][art]

Of course, in many ways, such layout can not fit in many cases. If you have
multiple and small files, it's probably not the best solution. At least,
it's one solution in the MirageOS eco-system!

[irmin]: https://github.com/mirage/irmin
[ocaml-git]: https://github.com/mirage/ocaml-git
[pack-file]: https://git-scm.com/docs/pack-format
[libXdiff]: http://www.xmailserver.org/xdiff-lib.html
[art]: https://github.com/dinosaure/art
