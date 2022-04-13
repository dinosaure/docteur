Simple disk.img
  $ docteur.make -d 2020-10-10T08:00:00+00:00 file://$(pwd)/store disk.img
  $ docteur.verify disk.img
  commit	: ca71a852f6c2d16bf04691f13b8a7edeadb15b80
  author	: "Dr. Greenthumb" <noreply@cypress.hill>
  root	: 7d8a9e386b602e1bd8cd8f226a461a87ccf372e3
  

  $ docteur.make -d 2020-10-10T08:00:00+00:00 relativize://store disk.img
  $ docteur.verify disk.img
  commit	: ca71a852f6c2d16bf04691f13b8a7edeadb15b80
  author	: "Dr. Greenthumb" <noreply@cypress.hill>
  root	: 7d8a9e386b602e1bd8cd8f226a461a87ccf372e3
  
