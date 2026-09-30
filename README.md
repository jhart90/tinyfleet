# TinyFleet: downloads and the online server

One Node server, no dependencies to install. Railway deploys it from this repo: it runs `npm start`
(`node server.mjs`) and supplies `PORT`.

- `/` is the download page, as it has always been: `index.html`, `version.json`, `favicon.svg`,
  `media/` (in ranges, for Safari) and `downloads/*.zip`, and nothing else from this folder
- `/play/` is the game itself (`play/`), and **Play online** there signs in to this server
- the same address takes the game's WebSocket; `/online.json` says how many are signed in
- `/health` and `/healthz` return `ok`, for Railway's health check

`server.mjs` and `play/` are made by `npm run online` in the game's project: don't edit them here.

## What Railway needs

- A **Postgres** database in the same project, and on this service the variable
  `DATABASE_URL` = `${{Postgres.DATABASE_URL}}`. Without it the online world is kept in files that the
  next deploy throws away.
- Optional: `PIN_PEPPER` (any long random text; set it before anyone signs up and never change it),
  `WORLD_SEED` (changing it later starts the world again), `MAX_PLAYERS` (100).

## A new release

In the game's project: `npm run package`, `npm run site` (the downloads) and `npm run online` (the
server and the game at /play/). Then commit and push this folder, and Railway redeploys. Anyone playing
online sees a reconnecting card for a few seconds; nothing is lost.

`media/hero.webm`, `hero.mp4`, `hero.jpg` are the header footage, filmed in the game at 1920×1080
(standard-range colour, as Chrome refuses full-range VP9), and its poster.
