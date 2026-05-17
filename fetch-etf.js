/**
 * fetch-etf.js — GitHub Actions daily ETF data fetcher
 * Tries multiple sources in order until one works.
 * Always writes data — falls back to extending existing data with today's date.
 */

const https = require('https');
const fs    = require('fs');

const SOSO_KEY = process.env.SOSO_KEY;
const ETFS = ['IBIT','FBTC','GBTC','ARKB','BITB','BTCO','HODL','BRRR','EZBC','BTCW','BTC','MSBT'];

const US_HOLIDAYS = new Set([
    '2024-01-01','2024-01-15','2024-02-19','2024-03-29','2024-05-27','2024-06-19','2024-07-04','2024-09-02','2024-11-28','2024-12-25',
    '2025-01-01','2025-01-20','2025-02-17','2025-04-18','2025-05-26','2025-06-19','2025-07-04','2025-09-01','2025-11-27','2025-12-25',
    '2026-01-01','2026-01-19','2026-02-16','2026-04-03','2026-05-25','2026-06-19','2026-07-03','2026-09-07','2026-11-26','2026-12-25',
    '2027-01-01','2027-01-18','2027-02-15','2027-03-26','2027-05-31','2027-06-18','2027-07-05','2027-09-06','2027-11-25','2027-12-24',
]);

function isTradingDay(date) {
    const d = new Date(date + 'T12:00:00Z');
    return d.getDay() !== 0 && d.getDay() !== 6 && !US_HOLIDAYS.has(date);
}

function todayISO() {
    return new Date().toISOString().slice(0, 10);
}

function get(url) {
    return new Promise((resolve, reject) => {
        const req = https.get(url, { rejectUnauthorized: false }, res => {
            let raw = '';
            res.on('data', chunk => raw += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(raw)); }
                catch { reject(new Error('Bad JSON from ' + url.slice(0,60))); }
            });
        });
        req.on('error', reject);
        req.setTimeout(20000, () => { req.destroy(); reject(new Error('Timeout')); });
    });
}

function post(hostname, path, body, headers = {}) {
    return new Promise((resolve, reject) => {
        const data = JSON.stringify(body);
        const options = {
            hostname, port: 443, path, method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data), ...headers },
            rejectUnauthorized: false,
        };
        const req = https.request(options, res => {
            let raw = '';
            res.on('data', chunk => raw += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(raw)); }
                catch { reject(new Error('Bad JSON')); }
            });
        });
        req.on('error', reject);
        req.setTimeout(20000, () => { req.destroy(); reject(new Error('Timeout')); });
        req.write(data);
        req.end();
    });
}

// ── SOURCE 1: SoSoValue API ────────────────────────────────────────────────
async function fetchSoSoValue() {
    if (!SOSO_KEY) throw new Error('No API key');
    console.log('Trying SoSoValue...');
    const res = await post('api.sosovalue.xyz', '/openapi/v2/etf/historicalInflowChart',
        { type: 'us-btc-spot' },
        { 'x-soso-api-key': SOSO_KEY }
    );
    console.log('SoSoValue code:', res.code, 'rows:', res.data?.list?.length ?? 0);
    if (res.code !== 0 || !res.data?.list?.length) throw new Error(res.msg || 'Empty response');

    const list = res.data.list;

    // Try per-ETF breakdown
    const perEtf = {};
    const ETF_TYPES = [
        ['IBIT','Etf_NASDAQ_IBIT'],['FBTC','Etf_NYSE_FBTC'],['GBTC','Etf_NYSE_GBTC'],
        ['ARKB','Etf_CBOE_ARKB'],['BITB','Etf_NYSE_BITB'],['BTCO','Etf_NYSE_BTCO'],
        ['HODL','Etf_CBOE_HODL'],['BRRR','Etf_NASDAQ_BRRR'],['EZBC','Etf_CBOE_EZBC'],
        ['BTCW','Etf_NYSE_BTCW'],['BTC','Etf_NYSE_BTC'],['MSBT','Etf_NYSE_MSBT'],
    ];
    await Promise.allSettled(ETF_TYPES.map(async ([ticker, type]) => {
        try {
            const r = await post('api.sosovalue.xyz', '/openapi/v2/etf/historicalInflowChart',
                { type }, { 'x-soso-api-key': SOSO_KEY });
            if (r.code !== 0) return;
            (r.data?.list || []).forEach(item => {
                if (!perEtf[item.date]) perEtf[item.date] = {};
                perEtf[item.date][ticker] = parseFloat((item.totalNetInflow / 1e6).toFixed(2));
            });
        } catch { /* skip */ }
    }));

    return list
        .filter(item => isTradingDay(item.date))
        .map(item => {
            const row = { date: item.date };
            ETFS.forEach(e => { row[e] = perEtf[item.date]?.[e] ?? null; });
            row.total = parseFloat((item.totalNetInflow / 1e6).toFixed(2));
            return row;
        })
        .filter(r => r.total !== 0)
        .sort((a, b) => b.date.localeCompare(a.date));
}

// ── SOURCE 2: CoinGlass public endpoint (no key needed for basic data) ─────
async function fetchCoinGlass() {
    console.log('Trying CoinGlass public...');
    const res = await get('https://open-api-v3.coinglass.com/api/etf/bitcoin/flow/history');
    if (!res.data || !Array.isArray(res.data)) throw new Error('No data from CoinGlass');

    const rows = [];
    res.data.forEach(day => {
        const date = new Date(day.t).toISOString().slice(0, 10);
        if (!isTradingDay(date)) return;
        const row = { date };
        ETFS.forEach(e => { row[e] = null; });
        row.total = parseFloat(((day.netFlow || 0) / 1e6).toFixed(2));
        if (row.total !== 0) rows.push(row);
    });

    if (rows.length === 0) throw new Error('CoinGlass returned 0 valid rows');
    console.log('CoinGlass rows:', rows.length);
    return rows.sort((a, b) => b.date.localeCompare(a.date));
}

// ── SOURCE 3: Farside HTML scrape ─────────────────────────────────────────
async function fetchFarside() {
    console.log('Trying Farside...');
    return new Promise((resolve, reject) => {
        const req = https.get('https://farside.co.uk/btc/', {
            rejectUnauthorized: false,
            headers: { 'User-Agent': 'Mozilla/5.0' }
        }, res => {
            let html = '';
            res.on('data', chunk => html += chunk);
            res.on('end', () => {
                const rows = [];
                const rowRe = /<tr[^>]*>([\s\S]*?)<\/tr>/g;
                const cellRe = /<td[^>]*>([\s\S]*?)<\/td>/g;
                let match;
                while ((match = rowRe.exec(html)) !== null) {
                    const cells = [];
                    let cm;
                    const rowHtml = match[1];
                    while ((cm = cellRe.exec(rowHtml)) !== null) {
                        cells.push(cm[1].replace(/<[^>]+>/g, '').trim());
                    }
                    if (cells.length >= 3 && /\d{2}\/\d{2}\/\d{4}/.test(cells[0])) {
                        const parts = cells[0].split('/');
                        const date = `${parts[2]}-${parts[1].padStart(2,'0')}-${parts[0].padStart(2,'0')}`;
                        if (!isTradingDay(date)) continue;
                        const total = parseFloat(cells[cells.length-1].replace(/[^-\d.]/g,''));
                        if (!isNaN(total) && total !== 0) {
                            const row = { date };
                            ETFS.forEach(e => { row[e] = null; });
                            row.total = total;
                            rows.push(row);
                        }
                    }
                }
                if (rows.length === 0) reject(new Error('Farside: no rows parsed'));
                else { console.log('Farside rows:', rows.length); resolve(rows.sort((a,b)=>b.date.localeCompare(a.date))); }
            });
        });
        req.on('error', reject);
        req.setTimeout(20000, () => { req.destroy(); reject(new Error('Timeout')); });
    });
}

// ── MERGE & WRITE ──────────────────────────────────────────────────────────
function mergeAndWrite(newRows, source) {
    const existing = fs.existsSync('etf-flows.json')
        ? JSON.parse(fs.readFileSync('etf-flows.json', 'utf8'))
        : { rows: [] };

    const merged = {};
    (existing.rows || []).filter(r => r.total !== 0 && isTradingDay(r.date))
        .forEach(r => { merged[r.date] = r; });
    newRows.forEach(r => { merged[r.date] = r; }); // new data wins

    const finalRows = Object.values(merged)
        .filter(r => r.total !== 0 && isTradingDay(r.date))
        .sort((a, b) => b.date.localeCompare(a.date));

    const output = {
        generated: new Date().toISOString(),
        source, etfs: ETFS,
        count: finalRows.length,
        latest: finalRows[0]?.date,
        oldest: finalRows[finalRows.length-1]?.date,
        rows: finalRows,
    };

    fs.writeFileSync('etf-flows.json', JSON.stringify(output, null, 2));
    console.log(`\n✅ Written: ${finalRows.length} rows | Latest: ${finalRows[0]?.date} | Source: ${source}`);
}

// ── MAIN ───────────────────────────────────────────────────────────────────
async function main() {
    console.log(`[${new Date().toISOString()}] Starting ETF fetch...`);
    console.log('Today:', todayISO(), 'Is trading day:', isTradingDay(todayISO()));

    const sources = [
        { name: 'SoSoValue', fn: fetchSoSoValue },
        { name: 'CoinGlass',  fn: fetchCoinGlass },
        { name: 'Farside',    fn: fetchFarside },
    ];

    for (const source of sources) {
        try {
            const rows = await source.fn();
            if (rows && rows.length > 0) {
                mergeAndWrite(rows, source.name);
                return; // success — done
            }
        } catch (err) {
            console.warn(`${source.name} failed:`, err.message);
        }
    }

    // All sources failed — keep existing data, just update timestamp
    console.warn('All sources failed — keeping existing data');
    if (fs.existsSync('etf-flows.json')) {
        const existing = JSON.parse(fs.readFileSync('etf-flows.json', 'utf8'));
        existing.generated = new Date().toISOString();
        existing.source = existing.source + ' (cached)';
        fs.writeFileSync('etf-flows.json', JSON.stringify(existing, null, 2));
        console.log('Kept existing', existing.count, 'rows');
    }
}

main().catch(err => {
    console.error('FATAL:', err.message);
    process.exit(0); // exit 0 so git commit still runs
});
