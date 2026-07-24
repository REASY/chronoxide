# Chronoxide slide deck

The deck is authored as a Marp document with locally bundled assets:

- `chronoxide.md` — editable source
- `chronoxide.pdf` — rendered handout
- `chronoxide.html` — rendered browser presentation
- `assets/JetBrainsMono-*.woff2` — reproducible monospace rendering
- `assets/JetBrainsMono-OFL.txt` — bundled font license

Pull the official renderer:

```sh
docker pull marpteam/marp-cli
```

Create a container-writable output directory:

```sh
install -d -m 0777 /tmp/chronoxide-marp-output
```

Render PDF:

```sh
docker run --rm --init \
  -v "$PWD:/home/marp/app:ro" \
  -v "/tmp/chronoxide-marp-output:/output" \
  -w /home/marp/app \
  marpteam/marp-cli \
  docs/slides/chronoxide.md \
  --allow-local-files \
  --html \
  --pdf \
  -o /output/chronoxide.pdf

cp /tmp/chronoxide-marp-output/chronoxide.pdf docs/slides/chronoxide.pdf
```

Render HTML:

```sh
docker run --rm --init \
  -v "$PWD:/home/marp/app:ro" \
  -v "/tmp/chronoxide-marp-output:/output" \
  -w /home/marp/app \
  marpteam/marp-cli \
  docs/slides/chronoxide.md \
  --allow-local-files \
  --html \
  -o /output/chronoxide.html

cp /tmp/chronoxide-marp-output/chronoxide.html docs/slides/chronoxide.html
```

The deck intentionally has no remote font, image, or JavaScript dependency.
