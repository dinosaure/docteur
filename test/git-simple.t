Simple Git disk.img
  $ mkdir git-store
  $ cd git-store
  $ git init -q 2> /dev/null
  $ git config init.defaultBranch master
  $ git config user.email "romain@mirage.io"
  $ git config user.name "Romain Calascibetta"
  $ echo "Hello Git!" > README.md
  $ git add README.md
  $ git commit -q -m .
  $ cd ..
  $ docteur.make file://$(pwd)/git-store disk.img
  $ cp disk.img ../unikernel/
  $ cd ../unikernel/
  $ mirage configure -t unix --disk disk.img
  $ mirage build 2> /dev/null > /dev/null
  $ ./simple --filename /README.md > out.result
  $ cd -
  $TESTCASE_ROOT
  $ diff ../unikernel/out.result git-store/README.md

