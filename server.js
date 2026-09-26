// The TinyFleet download page: a static server with no dependencies, for Railway (or anywhere Node runs).
// Railway gives the port in PORT; locally it's 8080. Zips are sent as downloads; the background video is
// served in ranges (Safari won't play a video without them); everything else as a page.
import { createServer } from 'node:http';
import { createReadStream, statSync } from 'node:fs';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = fileURLToPath(new URL('.', import.meta.url));
const PORT = Number(process.env.PORT) || 8080;
const TYPES = {
  '.html': 'text/html; charset=utf-8', '.json': 'application/json; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon', '.zip': 'application/zip', '.mp4': 'video/mp4', '.webm': 'video/webm',
};
// only these are served: the page, its data, its media and the downloads (never server.js or package.json)
const SERVED = /^\/(index\.html|version\.json|favicon\.svg|media\/[\w.-]+\.(mp4|webm|jpg)|downloads\/[\w.-]+\.zip)$/;

const text = (res, code, body) => res.writeHead(code, { 'content-type': 'text/plain' }).end(body);

createServer((req, res) => {
  let path;
  try { path = decodeURIComponent(new URL(req.url ?? '/', 'http://x').pathname); } catch { text(res, 400, 'Bad request'); return; }
  if (path === '/') path = '/index.html';
  if (path === '/health') { text(res, 200, 'ok'); return; }
  const file = normalize(join(ROOT, path));
  if (!SERVED.test(path) || !file.startsWith(ROOT)) { text(res, 404, 'Not found'); return; }
  let size;
  try { size = statSync(file).size; } catch { text(res, 404, 'Not found'); return; }
  const ext = extname(file);
  const head = { 'content-type': TYPES[ext] ?? 'application/octet-stream', 'accept-ranges': 'bytes' };
  if (ext === '.zip') head['content-disposition'] = `attachment; filename="${path.split('/').pop()}"`;
  // media rarely changes between releases and is big: cached for a day; the rest is checked each visit
  head['cache-control'] = path.startsWith('/media/') ? 'public, max-age=86400' : 'no-cache';
  // a range: the part of the file asked for
  const range = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range ?? '');
  if (range && (range[1] || range[2])) {
    let start = range[1] ? Number(range[1]) : size - Number(range[2]);
    let end = range[1] && range[2] ? Number(range[2]) : size - 1;
    start = Math.max(0, start); end = Math.min(size - 1, end);
    if (start > end || start >= size) { res.writeHead(416, { 'content-range': `bytes */${size}` }).end(); return; }
    res.writeHead(206, { ...head, 'content-range': `bytes ${start}-${end}/${size}`, 'content-length': end - start + 1 });
    if (req.method === 'HEAD') { res.end(); return; }
    createReadStream(file, { start, end }).pipe(res);
    return;
  }
  res.writeHead(200, { ...head, 'content-length': size });
  if (req.method === 'HEAD') { res.end(); return; }
  createReadStream(file).pipe(res);
}).listen(PORT, () => console.log(`TinyFleet downloads on :${PORT}`));
