# Diagrams in this repo

A workflow has been set up to automatically render any PlantUML files
(with extension `.puml`).

You can put them anywhere in the source tree and they will be rendered
into their equivalent `.svg` file.

Eg.

You have file `docs/foo/bar.md`

You wish to add a diagram, so create `docs/foo/my_diagram.puml`

On push to any branch, the svg will be created: `docs/foo/my_diagram.svg`

In the markdown add the line to reference the diagram:

```markdown
![My diagram](my_diagram.svg)
```

There are plugins available for most IDEs so you can preview the `.puml` file while you write it.