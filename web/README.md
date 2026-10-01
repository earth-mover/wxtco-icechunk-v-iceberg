# Blog chart widgets

Interactive charts for the blog post "Weather Forecast Data: Icechunk vs. Iceberg Head-to-Head".
Four custom elements, one dependency-free ES module, one generated data module.

| Element | What it shows | Controls |
|---|---|---|
| `<wxtco-storage>` | bytes stored per method | unit (bytes / $ per month), include the NetCDF copy |
| `<wxtco-ingest>` | time and cost to ingest one cycle | method chips, unit (min per cycle / $ per cycle / $ per month) |
| `<wxtco-queries>` | Q1, Q2, Q3 as small multiples | method chips with presets, unit (seconds / $ per 1,000 queries) |
| `<wxtco-tco>` | monthly cost stacked by storage, ingest, Q1, Q2, Q3 | four workload sliders, 1x/3x/10x presets, NetCDF copy kept or deleted, method chips |

Every widget has a hover and keyboard-focus tooltip, a "Lower is better" hint in its legend, and a "Show table" toggle with the same numbers.
Selections persist per browser in `localStorage`.

## Files

- `wxtco-charts.js`: the module. Injects its own CSS once; inherits the page font.
- `wxtco-data.js`: generated from `docs/findings/` by `scripts/export_web_data.py`. Do not edit by hand.
- `index.html`, `storage.html`, `ingest.html`, `queries.html`, `tco.html`: standalone pages for review.

Regenerate the data after any change to the findings CSVs or `prices.toml`:

```
uv run python scripts/export_web_data.py
```

Preview locally (ES modules need http, not `file://`):

```
cd web && python3 -m http.server 8765
# open http://localhost:8765/index.html
```

## Why this approach

The Earthmover site is Astro 5 with MDX and Tailwind 4, static output, no client framework, and no
charting library. Posts already embed raw HTML and `<script>` tags directly in `.md` files, and one
post uses an `.astro` component that wraps a raw HTML file. Custom elements fit that pattern:

- No new npm dependency. Astro ships zero client JS by default; a chart library would be the largest
  script on the page. The module is ~34 KB unminified with no runtime dependencies.
- Same code runs standalone and in the blog. The standalone pages here and the blog post load the
  identical file, so what you review is what ships.
- Works in `.md` as well as `.mdx`. A custom element is just an HTML tag; no component import needed.
- Brand-aligned by construction. Colors are darker steps of the brand accents (violet, red, blue,
  orange, pink) chosen with the dataviz palette validator so adjacent stacked segments stay distinct
  under colour-vision deficiency and clear 3:1 contrast on white. Text uses the site's ink tokens.

## Embedding in the Astro site

1. Copy the two modules into the site's static folder:

   ```
   mkdir -p ../design2/website/public/wxtco
   cp web/wxtco-charts.js web/wxtco-data.js ../design2/website/public/wxtco/
   ```

2. In the post (`src/content/blog/weather-forecast-data-icechunk-vs-iceberg.md`), load the module once
   and place the elements where the charts go:

   ```html
   <script type="module" src="/wxtco/wxtco-charts.js"></script>

   ### Storage Cost
   <wxtco-storage></wxtco-storage>

   ### Ingestion Cost and Latency
   <wxtco-ingest></wxtco-ingest>

   ### Query Cost and Performance
   <wxtco-queries></wxtco-queries>

   ### TCO Calculator
   <wxtco-tco></wxtco-tco>
   ```

   Astro passes raw HTML in Markdown through untouched, so this works in the existing `.md` file.
   Leave a blank line before and after each tag so the Markdown parser treats it as a block.

3. Optional: an `.astro` wrapper if you prefer imports over a script tag. Create
   `src/components/TcoCharts.astro`:

   ```astro
   ---
   // Loads the widget module once; place any <wxtco-*> element in the post body.
   ---
   <script>
     import '/wxtco/wxtco-charts.js';
   </script>
   ```

   Then rename the post to `.mdx`, `import TcoCharts from '../../components/TcoCharts.astro';`, and
   render `<TcoCharts />` once near the top. Astro bundles and deduplicates the script.

The widgets read the article's font and sit on a white card with the site's hairline border, matching
the existing figure treatment in `.prose-light`. They are 100% wide; the `.prose-light` container
already caps line length so no extra wrapper is needed. Below about 500 px each plot scrolls
horizontally so the six category labels never collide.

## Cost model

`wxtco-charts.js` recomputes costs client-side with the same formulas as `src/wxtco/tco.py`:

- storage: GB x $0.023 per GB-month, plus the 6,494 GB NetCDF copy when selected
- ingest: median seconds per cycle / 3600 x c7i.16xlarge hourly price x cycles per day x 30
- each query: (median seconds / 3600 x m7i.4xlarge hourly price + S3 requests per run at list price) x runs per day x 30,
  where requests per run come from `docs/findings/requests.csv` (GET and HEAD at $0.0004, LIST at $0.005 per 1,000)

Q3 is the batched stream at B=8, priced per 16-sample job. Wire bytes in the tooltips are the node's
received bytes per run from the canonical benchmark pass.
