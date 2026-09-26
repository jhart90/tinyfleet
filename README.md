# TinyFleet downloads

The download page for TinyFleet: Windows and Mac builds, served by a small Node server with no
dependencies. Railway deploys it from this repo: it runs `npm start` and supplies `PORT`.

- `index.html`: the page
- `server.js`: serves the page, `version.json`, `favicon.svg`, `media/` (in ranges, for Safari) and `downloads/*.zip` only
- `media/hero.webm`, `hero.mp4`, `hero.jpg`: the header footage, filmed in the game at 1920×1080 (7 shots, 42 s loop) and its poster
- `downloads/`: `TinyFleet-Windows.zip` and `TinyFleet-Mac.zip`
- `/health` returns `ok`, for a Railway health check

## A new release

In the game's project: `npm run package`, then `npm run site`. Then commit and push this folder,
and Railway redeploys.
