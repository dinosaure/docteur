Simple Git disk.img
  $ mkdir git-store
  $ cd git-store
  $ git init -q 2> /dev/null
  $ git checkout -q -b master
  $ git config init.defaultBranch master
  $ git config user.email "romain@mirage.io"
  $ git config user.name "Romain Calascibetta"
  $ echo "Hello Git!" > README.md
  $ git add README.md
  $ export DATE="2016-08-21 17:18:43 +0200"
  $ export GIT_COMMITTER_DATE="2016-08-21 17:18:43 +0200"
  $ git commit --no-gpg-sign --date "$DATE" -q -m "My name is Dr. Greenthumb"
  $ cd ..
  $ docteur.make --branch refs/heads/master file://$(pwd)/git-store disk.img
  $ docteur.verify disk.img
  commit	: 7f76ddedf89aaca1a566a5cb3fb2f1b5cc96f716
  author	: "Romain Calascibetta" <romain@mirage.io>
  root	: 73c44169dbb57613737326b902934762298208d4
  
  My name is Dr. Greenthumb
  
