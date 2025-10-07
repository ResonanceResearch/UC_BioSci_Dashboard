// dashboard.js — full version with OpenAlex profile links and FWCI in publications
(function(){
  // ==== Defaults ====
  const DEFAULT_START_YEAR = 2021;
  const DEFAULT_END_YEAR = 2025;

  document.addEventListener('DOMContentLoaded', () => {
    // Paths used by index.html
    const rosterPath = 'data/roster_with_metrics.csv';
    const pubsPath = 'data/openalex_all_authors_last5y_key_fields_dedup.csv';
    const authorshipsPath = 'data/openalex_all_authors_last5y_key_fields.csv'; // pre-dedup, optional
    const perAuthorPath = 'data/openalex_all_authors_last5y_key_fields_dedup_per_author.csv';  // NEW
    
    // In-memory data
    let rosterData = [];   // faculty roster + metrics
    let pubData = [];      // publications (last 5y) — dedup (one row per work)
    let perAuthorData = []; // NEW: per-author projection (one row per (work, cohort-author))
    let yearBounds = { min: DEFAULT_START_YEAR, max: DEFAULT_END_YEAR };
    let authorshipData = null;

    // Focus (single author) state
    let focusedAuthorID = null;
    let focusedAuthorName = '';
    let lastSelectedPubs = []; // holds the most recent filtered publications

    // ---- Publication Type selection state ----
    const DEFAULT_TYPE_SET = new Set(['article', 'book', 'book-chapter', 'review']);
    // We normalize OpenAlex types (e.g., journal-article -> article; review-article -> review)
    let activeTypes = null; // null = "not chosen yet" -> will be set to DEFAULT_TYPE_SET on first use

    function normalizePubType(t) {
      const s = String(t || '').toLowerCase().trim();
      if (s === 'journal-article' || s === 'journal article') return 'article';
      if (s === 'review-article'  || s === 'review article')  return 'review';
      return s;
    }

    // Helper: current active types from legend (or default)
    function gatherActiveTypes(){
      // Use state if present
      if (typeof activeTypes !== 'undefined' && activeTypes && activeTypes.size) {
        return new Set(activeTypes);
      }
      // Else read legend labels if available
      try {
        const vis = getActiveTypeLabelsFromLegend(); // returns ['article', ...]
        if (vis && vis.length) return new Set(vis.map(s => String(s).toLowerCase()));
      } catch(_) {}
      // Fallback to defaults
      return new Set(['article','book','book-chapter','review']);
    }

    
    // Load both CSVs, then initialize
    Promise.all([
      fetchCSV(rosterPath),
      fetchCSV(pubsPath),
      fetchCSVIfExists(authorshipsPath),
      fetchCSVIfExists(perAuthorPath)])
      .then(([rosterCSV, pubsCSV, authCSV, perAuthorCSV]) => {
      rosterData = parseCSV(rosterCSV);
      pubData = parseCSV(pubsCSV);
      authorshipData = authCSV ? parseCSV(authCSV) : [];
      perAuthorData = perAuthorCSV ? parseCSV(perAuthorCSV) : [];  // NEW

      normalizeRoster();
      normalizePubsFor(pubData);
      if (perAuthorData && perAuthorData.length) normalizePubsFor(perAuthorData);
      // Optional: normalize authorship, if you want to coerce types/columns fancily
      // (Not strictly necessary; we normalize at use-time.)

     // Ensure the network can draw immediately on first render:
     
      initFilters();
      initYearInputs();
      bindEvents();
      ensurePcaSearchUI();
      update();                 // IMPORTANT: forces initial render (fixes “needs a filter change”)
      document.getElementById('loading-banner')?.classList.add('hidden');
      }).catch(err => console.error('Failed to load CSVs', err));


    // ============ Core helpers ============
    function fetchCSVIfExists(path){
      return fetch(path).then(r => r.ok ? r.text() : null).catch(() => null);
      }
    
    function toInt(x) {
      const n = Number(x);
      return Number.isFinite(n) ? Math.round(n) : 0;
    }
    function toFloat(x) {
      const n = Number(x);
      return Number.isFinite(n) ? n : NaN;
    }
    function clampYear(y){
      y = toInt(y);
      if (!y) return DEFAULT_START_YEAR; // avoid 0 showing up
      if (y < DEFAULT_START_YEAR) return DEFAULT_START_YEAR;
      if (y > DEFAULT_END_YEAR) return DEFAULT_END_YEAR;
      return y;
    }
    function normalizeID(id) {
      // Accept raw OpenAlex IDs or full URLs
      return String(id || '')
        .replace(/^https?:\/\/openalex\.org\/authors\//i, '')
        .replace(/^https?:\/\/openalex\.org\//i, '')
        .trim();
    }
    function fetchCSV(path) { return fetch(path).then(resp => resp.text()); }

    function wc_collectTermsBySource(pubs, source) {
      // Count each term at most once per publication (document frequency)
      const counts = new Map();
    
      pubs.forEach(p => {
        let terms = [];
    
        if (source === 'topics') {
          const t1 = (p.primary_topic__display_name || '').trim();
          const t2 = (p.primary_topic__subfield__display_name || '').trim();
          terms = [t1, t2].filter(Boolean);
        } else {
          // concepts (default)
          const raw = p.concepts_list || '';
          // split by | ; ,  (covers most CSV encodings)
          terms = raw.split(/[\|;,]/).map(s => s.trim()).filter(Boolean);
        }
    
        // normalize to lowercase; count each term once per paper
        const uniq = new Set(terms.map(t => t.toLowerCase()));
        uniq.forEach(t => counts.set(t, (counts.get(t) || 0) + 1));
      });
    
      return counts;
    }
    
    function wc_buildList(pubs, source, limit) {
      const counts = wc_collectTermsBySource(pubs, source);
      const sorted = Array.from(counts.entries())
        .sort((a,b) => b[1] - a[1])
        .slice(0, Math.max(5, Math.min(100, limit || 100)));
      // wordcloud2.js wants [text, weight] pairs
      return sorted.map(([text, weight]) => [text, weight]);
    }

    
    // CSV parser that handles quoted commas and quotes
    function parseCSV(text) {
      const lines = text.replace(/\r/g, '').split('\n').filter(Boolean);
      if (!lines.length) return [];
      const headers = splitCSVLine(lines.shift());
      return lines.map(line => {
        const values = splitCSVLine(line);
        const row = {};
        headers.forEach((h, i) => {
          const key = h.trim();
          let v = values[i] !== undefined ? values[i] : '';
          // Strip surrounding quotes if present
          v = v.replace(/^"|"$/g, '').replace(/""/g, '"');
          row[key] = v;
        });
        return row;
      });
    }
    function splitCSVLine(line) {
      const out = [];
      let cur = '';
      let inQuotes = false;
      for (let i=0;i<line.length;i++){
        const ch = line[i];
        if (ch === '"'){
          if (inQuotes && line[i+1] === '"'){ cur += '"'; i++; }
          else { inQuotes = !inQuotes; }
        } else if (ch === ',' && !inQuotes) {
          out.push(cur);
          cur = '';
        } else cur += ch;
      }
      out.push(cur);
      return out;
    }

    // Minimal ISO2 -> ISO3 (extend as needed)
    const ISO2_TO_3 = {
      US:'USA', CA:'CAN', GB:'GBR', FR:'FRA', DE:'DEU', NL:'NLD', BE:'BEL', AU:'AUS',
      CN:'CHN', IN:'IND', BR:'BRA', ZA:'ZAF', SE:'SWE', NO:'NOR', DK:'DNK', FI:'FIN',
      IT:'ITA', ES:'ESP', PT:'PRT', CH:'CHE', JP:'JPN', KR:'KOR', IL:'ISR', IE:'IRL',
      NZ:'NZL', MX:'MEX', AR:'ARG', RU:'RUS', TR:'TUR', SA:'SAU', AE:'ARE', SG:'SGP',
      HK:'HKG',
      AT:'AUT', PL:'POL', CZ:'CZE', HU:'HUN', GR:'GRC', RO:'ROU',
      TW:'TWN', TH:'THA', MY:'MYS', ID:'IDN', PK:'PAK', BD:'BGD',
      EG:'EGY', NG:'NGA', KE:'KEN', IR:'IRN',
      CL:'CHL', CO:'COL', PE:'PER', CU:'CUB',
      KN:'KNA', 
      GL:'GRL'  
    };

    function toISO3(code){
      if (!code) return '';
      const c = String(code).trim().toUpperCase();
      return c.length === 3 ? c : (ISO2_TO_3[c] || '');
    }

    // ISO-3 → Full country name (covers all codes you listed)
    const ISO3_TO_NAME = {
      USA:'United States', CAN:'Canada', GBR:'United Kingdom', FRA:'France', DEU:'Germany', NLD:'Netherlands', BEL:'Belgium', AUS:'Australia',
      CHN:'China', IND:'India', BRA:'Brazil', ZAF:'South Africa', SWE:'Sweden', NOR:'Norway', DNK:'Denmark', FIN:'Finland',
      ITA:'Italy', ESP:'Spain', PRT:'Portugal', CHE:'Switzerland', JPN:'Japan', KOR:'South Korea', ISR:'Israel', IRL:'Ireland',
      NZL:'New Zealand', MEX:'Mexico', ARG:'Argentina', RUS:'Russia', TUR:'Turkey', SAU:'Saudi Arabia', ARE:'United Arab Emirates', SGP:'Singapore',
      HKG:'Hong Kong', AUT:'Austria', POL:'Poland', CZE:'Czechia', HUN:'Hungary', GRC:'Greece', ROU:'Romania',
      TWN:'Taiwan', THA:'Thailand', MYS:'Malaysia', IDN:'Indonesia', PAK:'Pakistan', BGD:'Bangladesh',
      EGY:'Egypt', NGA:'Nigeria', KEN:'Kenya', IRN:'Iran', CHL:'Chile', COL:'Colombia', PER:'Peru', CUB:'Cuba', KNA:'Saint Kitts and Nevis', GL:'Greenland'  
    };

    function countryName(iso3) {
      const k = String(iso3 || '').toUpperCase();
      return ISO3_TO_NAME[k] || k;  // graceful fallback to code
    }
    
    // Parse a pipe-separated field safely -> array
    function splitPipe(v){
      if (!v) return [];
      return String(v).split('|').map(s => s.trim());
    }
    
    
    // Extract aligned authorship arrays from a publication row (now with institutions)
    function parseAuthorships(pub){
      // Base aligned arrays
      const countries = splitPipe(pub['authorships__countries']).map(toISO3);
      const ids       = splitPipe(pub['authorships__author__id']).map(s => s.replace(/^https?:\/\/openalex\.org\/authors\//i,''));
      const names     = splitPipe(pub['authorships__author__display_name']).length
                      ? splitPipe(pub['authorships__author__display_name'])
                      : splitPipe(pub['authorships__raw_author_name']);
      const corresRaw = splitPipe(pub['authorships__is_corresponding']);
      const pos       = splitPipe(pub['authorships__author_position']); // first|middle|last
    
      // NEW: institution fields (pipe-delimited per authorship; within each, OpenAlex may have multiple institutions)
      // We take the FIRST institution per authorship as the representative (common practice).
      const instRorRaw   = splitPipe(pub['authorships__institutions__ror']);           // e.g. "https://ror.org/01xxx;https://ror.org/02yyy|..."
      const instNameRaw  = splitPipe(pub['authorships__institutions__display_name']);  // e.g. "Univ X;Inst Y|..."
      const instCtryRaw  = splitPipe(pub['authorships__institutions__country_code']);  // e.g. "CA;US|..."
    
      const firstFromSemicolon = (cell) => {
        if (!cell) return '';
        const s = String(cell).split(';')[0].trim();
        return s;
      };
    
      const instRor  = instRorRaw.map(c => firstFromSemicolon(c).replace(/^https?:\/\/ror\.org\//i,''));
      const instName = instNameRaw.map(c => firstFromSemicolon(c));
      const instISO2 = instCtryRaw.map(c => firstFromSemicolon(c).toUpperCase());
      const instISO3 = instISO2.map(toISO3);
    
      // Normalize corresponding flags to booleans, pad arrays to same length
      const isCorresponding = corresRaw.map(x => {
        const s = String(x || '').toLowerCase();
        return (s === 'true' || s === '1' || s === 'yes' || s === 'y');
      });
    
      const n = countries.length;
      const pad = (arr, fill='') => (arr.length === n ? arr : Array.from({length:n}, (_,i)=> arr[i] ?? fill));
    
      return {
        countries,
        ids: pad(ids),
        names: pad(names),
        isCorresponding: pad(isCorresponding, false),
        position: pad(pos, ''),
        instRor: pad(instRor),
        instName: pad(instName),
        instCountry3: pad(instISO3)
      };
    }

    
    // Infer the cohort's "home" country (ISO-3) from the current selection.
    // We tally the affiliation countries of cohort authors across selected pubs,
    // then pick the most frequent. Falls back to 'CAN' if nothing tallies.
    function computeHomeCountry(contributingRoster, selectedPubs){
      const cohortIDs = new Set(
        (contributingRoster || []).map(r =>
          String(r.OpenAlexID || '').replace(/^https?:\/\/openalex\.org\/authors\//i,'')
        )
      );
      const tallies = new Map(); // iso3 -> count
    
      for (const p of (selectedPubs || [])) {
        const a = parseAuthorships(p);
        for (let i = 0; i < a.ids.length; i++) {
          const aid = a.ids[i];
          const cty = a.countries[i];
          if (!aid || !cty) continue;
          if (cohortIDs.has(aid)) {
            tallies.set(cty, (tallies.get(cty) || 0) + 1);
          }
        }
      }
    
      if (!tallies.size) return 'CAN';
      let best = null, bestN = -1;
      for (const [k, n] of tallies.entries()) {
        if (n > bestN) { best = k; bestN = n; }
      }
      return best || 'CAN';
    }

    // Infer the cohort's "home" ROR from the current selection.
    // We tally the FIRST institution ROR per cohort authorship (normalized in parseAuthorships)
    // and return the most frequent one.
    function computeHomeROR(contributingRoster, selectedPubs){
      const cohortIDs = new Set(
        (contributingRoster || []).map(r =>
          String(r.OpenAlexID || '').replace(/^https?:\/\/openalex\.org\/authors\//i, '')
        )
      );
      const tallies = new Map(); // ror -> count
    
      for (const p of (selectedPubs || [])) {
        const a = parseAuthorships(p);                  // has a.instRor[], a.ids[]
        const n = a.ids.length;
        for (let i = 0; i < n; i++) {
          const id  = a.ids[i];
          const ror = (a.instRor[i] || '').trim();      // already stripped of https://ror.org/
          if (!id || !ror) continue;
          if (cohortIDs.has(id)) {
            tallies.set(ror, (tallies.get(ror) || 0) + 1);
          }
        }
      }
    
      let best = '', bestN = 0;
      for (const [k, n] of tallies.entries()) {
        if (n > bestN) { best = k; bestN = n; }
      }
      return best; // '' if nothing seen
    }
    
    function workKey(p){
      // prefer OpenAlex work id, else DOI, else normalized title
      const id = String(p.id || '').replace(/^https?:\/\/openalex\.org\//i,'').trim();
      if (id) return 'id:' + id;
      const doi = String(p.doi || '').replace(/^https?:\/\/(dx\.)?doi\.org\//i,'').toLowerCase().trim();
      if (doi) return 'doi:' + doi;
      return 't:' + normalizeText(String(p.display_name || ''));
    }

    // ============ Compute Countries ============

    // NEW: position-aware international link computation
    function computeIntlLinks(selectedPubs, contributingRoster, homeISO3) {
      const counts = new Map(); // iso3 -> per-paper counts
    
      // cohort OpenAlex IDs present in the current selection
      const cohortIDs = new Set(
        (contributingRoster || []).map(r =>
          String(r.OpenAlexID || '').replace(/^https?:\/\/openalex\.org\/authors\//i,'')
        )
      );
    
      for (const p of (selectedPubs || [])) {
        const a = parseAuthorships(p);  // aligned arrays
        const n = a.ids.length;
        if (!n) continue;
    
        // Collect indices by role & country relative to home
        const cohortIdx_FL = [];
        const cohortIdx_M  = [];
        const intlIdx_any  = [];
        const intlIdx_FL   = [];
    
        for (let i = 0; i < n; i++) {
          const id  = a.ids[i];
          const pos = String(a.position[i] || '').toLowerCase(); // "first" | "middle" | "last"
          const cty = a.countries[i];
    
          const isCohort = id && cohortIDs.has(id);
          const isIntl   = cty && homeISO3 && cty !== homeISO3;
          const isFL     = (pos === 'first' || pos === 'last');
    
          if (isCohort && isFL) cohortIdx_FL.push(i);
          else if (isCohort && pos === 'middle') cohortIdx_M.push(i);
    
          if (isIntl) {
            intlIdx_any.push(i);
            if (isFL) intlIdx_FL.push(i);
          }
        }
    
        // Countries credited on this paper (set so each country counts once/paper)
        const creditedThisPaper = new Set();
    
        // Rule 1: cohort FL × any international coauthor (any position)
        if (cohortIdx_FL.length && intlIdx_any.length) {
          for (const j of intlIdx_any) {
            const iso = a.countries[j];
            if (iso) creditedThisPaper.add(iso);
          }
        }
    
        // Rule 2: cohort M × international FL
        if (cohortIdx_M.length && intlIdx_FL.length) {
          for (const j of intlIdx_FL) {
            const iso = a.countries[j];
            if (iso) creditedThisPaper.add(iso);
          }
        }
    
        // Tally
        for (const iso of creditedThisPaper) {
          counts.set(iso, (counts.get(iso) || 0) + 1);
        }
      }
    
      const iso3 = Array.from(counts.keys()).sort();
      const values = iso3.map(k => counts.get(k));
      const total = values.reduce((a,b)=>a+b, 0);
      return { iso3, values, total };
    }


    
    // ============ Normalization ============
    function normalizeRoster(){
      // Normalize OpenAlexID, collect research groups and numeric metrics
      rosterData.forEach(r => {
        r.OpenAlexID = normalizeID(r.OpenAlexID);
        r.Name = r.Name || '';
        // combine RG1..RG4 into a single array for filtering
        r._RGs = ['RG1','RG2','RG3','RG4']
          .map(k => (r[k] || '').trim())
          .filter(v => v);
        r.H_index = toInt(r.H_index);
        r.I10_index = toInt(r.I10_index);
        r.Works_count = toInt(r.Works_count);
        r.Total_citations = toInt(r.Total_citations);
      });
    }

    function normalizePubsFor(arr){
      // Map cohort author OpenAlexID -> array of name tokens (first, middle, last pieces)
      const cohortTokensByID = new Map(
        (rosterData || []).map(r => {
          const id = normalizeID(r.OpenAlexID);
          const toks = normalizeText(r.Name || '').split(/\s+/).filter(Boolean);
          return [id, toks];
        })
      );
    
      arr.forEach(p => {
        // numeric
        p.publication_year = clampYear(p.publication_year);
        p.cited_by_count = toInt(p.cited_by_count);
    
        // author ID normalization
        p.author_openalex_id = normalizeID(p.author_openalex_id);
    
        // DOI -> full URL if needed
        if (p.doi && !/^https?:\/\//i.test(p.doi)) {
          p.doi = 'https://doi.org/' + p.doi;
        }
    
        // FWCI cache (numeric)
        p._fwci = toFloat(p.fwci);
    
        // normalized type for filtering
        p._type_norm = normalizePubType(p.type);
    
        // topic haystack for search (title + topics + concepts)
        const cParts = [
          p.concepts_list || '',
          p.primary_topic__subfield__display_name || '',
          p.primary_topic__display_name || '',
          p.display_name || ''
        ];
    
        // >>> Append cohort author name tokens present on this work <<<
        // Prefer union list if present; else fall back to authorships ids
        const union = String(p.cohort_union_author_ids || '').trim();
        let ids = union ? union.split('|').map(normalizeID).filter(Boolean) : [];
        if (!ids.length) {
          ids = splitPipe(p['authorships__author__id']).map(normalizeID).filter(Boolean);
        }
    
        const nameTokens = [];
        for (const aid of ids) {
          const toks = cohortTokensByID.get(aid);
          if (toks && toks.length) nameTokens.push(...toks);
        }
        if (nameTokens.length) cParts.push(nameTokens.join(' '));
    
        // Final haystack
        p._topic_haystack = normalizeText(cParts.join(' ').toLowerCase());
      });
    }



    // ============ UI: filters, years, events ============
    function initFilters(){
      // Populate multiselects for Level, Category, Appointment, Research Group
      const levelSel = document.getElementById('level');
      const catSel = document.getElementById('category');
      const apptSel = document.getElementById('appointment');
      const rgSel = document.getElementById('research-group');

      fillSelect(levelSel, uniqueNonEmpty(rosterData.map(r => r.Level || '')));
      fillSelect(catSel, uniqueNonEmpty(rosterData.map(r => r.Category || '')));
      fillSelect(apptSel, uniqueNonEmpty(rosterData.map(r => r.Appointment || '')));

      // Default Appointment selection: ONLY Full‑time selected
      setDefaultAppointmentSelection();

      // Research groups across RG1..RG4
      const allRGs = new Set();
      rosterData.forEach(r => r._RGs.forEach(g => allRGs.add(g)));
      fillSelect(rgSel, Array.from(allRGs).sort());

      // === PROMOTE: Reset to defaults button ===
      (function promoteResetButton(){
        // Find a button whose label matches "Reset to defaults" (case-insensitive, trims whitespace)
        const allBtns = Array.from(document.querySelectorAll('button, [role="button"], input[type="button"], input[type="submit"]'));
        const target = allBtns.find(el => {
          const label = (el.innerText || el.value || '').trim().toLowerCase();
          return label === 'reset to defaults';
        });
      
        if (target) {
          target.classList.add('btn-reset-prominent');
        }
      })();


      
    }

    // Select only "Full‑time" in the Appointment multi-select, deselect others.
    // Tolerant to case and optional hyphen/space (e.g., Full time, Full-time).
    function setDefaultAppointmentSelection(){
      const sel = document.getElementById('appointment');
      if (!sel) return;
      const isFullTime = (s) => /full\s*-?\s*time/i.test(String(s || ''));
      for (const opt of sel.options) {
        opt.selected = isFullTime(opt.value) || isFullTime(opt.text);
      }
    }

    function fillSelect(sel, options){
      sel.innerHTML = '';
      options.forEach(v => {
        const opt = document.createElement('option');
        opt.value = v;
        opt.textContent = v;
        sel.appendChild(opt);
      });
    }

    function initYearInputs(){
      const ymin = document.getElementById('year-min');
      const ymax = document.getElementById('year-max');
      ymin.value = DEFAULT_START_YEAR;
      ymax.value = DEFAULT_END_YEAR;
      yearBounds = { min: DEFAULT_START_YEAR, max: DEFAULT_END_YEAR };
    }

    function downloadDataURL(filename, dataURL) {
      const a = document.createElement('a');
      a.href = dataURL;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
    }
    
    function downloadBlob(filename, blob) {
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1500);
    }

    // Export a Plotly div as PNG or SVG (includes current legend visibility)
    async function exportPlotlyImage(divId, fmt, filenameBase) {
      const el = document.getElementById(divId);
      if (!el || !el.data) return; // guard: chart not rendered yet
      const format = (fmt === 'svg' ? 'svg' : 'png');
      // Use current on-screen size for WYSIWYG; scale=2 for sharper images
      const w = el.clientWidth || 900;
      const h = el.clientHeight || 500;
      const dataUrl = await Plotly.toImage(el, { format, width: w, height: h, scale: 2 });
      downloadDataURL(`${filenameBase}.${format}`, dataUrl);
    }
    
    // Export a standalone interactive HTML snapshot of a Plotly chart (keeps hover)
    function exportPlotlyHTML(divId, filenameBase, title = 'Figure') {
      const el = document.getElementById(divId);
      if (!el || !el.data) return; // guard: chart not rendered yet
      const figData = JSON.stringify(el.data);
      const figLayout = JSON.stringify(el.layout || {});
      const tpl = `<!doctype html>
    <html>
    <head>
    <meta charset="utf-8"><title>${title}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>html,body,#plot{height:100%;margin:0}</style>
    </head>
    <body>
    <div id="plot"></div>
    <script>
    const data = ${figData};
    const layout = ${figLayout};
    Plotly.newPlot('plot', data, layout, {responsive:true});
    </script>
    </body>
    </html>`;
      downloadBlob(`${filenameBase}.html`, new Blob([tpl], { type: 'text/html' }));
    }

    
    // Rebuild the current word list (mirrors renderWordCloud’s logic)
    function _currentWordListForExport() {
      const pubs = _wc_lastPubs.length ? _wc_lastPubs : [];
      const source = document.querySelector('input[name="wc-source"]:checked')?.value || 'concepts';
      let filtered = pubs;
      try {
        const el = document.getElementById('pub-chart');
        const visible = new Set((el?.data || [])
          .filter(tr => tr.visible === true || tr.visible === undefined)
          .map(tr => String(tr.name).toLowerCase()));
        if (visible.size) {
          filtered = pubs.filter(p => visible.has(String((p.type || 'other')).toLowerCase()));
        }
      } catch(_) {}
      return wc_buildList(filtered, source, 100);
    }
    
    // Ensure the word cloud canvas exists without triggering a full dashboard update.
    // If needed, it draws the cloud from the current filtered pubs, then calls cb()
    // after the canvas is actually present and laid out.
    function _ensureWordCloudThen(cb) {
      const container = document.getElementById('wordcloud');
      if (!container) return;
    
      // If a canvas already exists, export immediately
      const haveCanvas = !!container.querySelector('canvas');
      if (haveCanvas) { cb(); return; }
    
      // Draw from the most recent pubs set; if empty, compute just the filtered pubs
      const pubs = (_wc_lastPubs && _wc_lastPubs.length)
        ? _wc_lastPubs
        : (applyFilters().selectedPubs || []);
    
      renderWordCloud(pubs);
    
      // Give wordcloud2.js time to paint the canvas before we snapshot
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          const ready = !!container.querySelector('canvas');
          if (ready) cb();
          // (Optional) else: you could retry once more after a short timeout
          // else setTimeout(() => { if (container.querySelector('canvas')) cb(); }, 60);
        });
      });
    }

    
    // Export a minimal interactive HTML that rebuilds with current list
    function exportWordCloudHTML(filenameBase='word_cloud') {
      const list = _currentWordListForExport();
      const tpl = `<!doctype html>
    <html>
    <head>
    <meta charset="utf-8"><title>Word Cloud</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/wordcloud2.js/1.2.2/wordcloud2.min.js"></script>
    <style>html,body,#wc{height:100%;margin:0} #wc{background:#fff}</style>
    </head>
    <body>
    <div id="wc"></div>
    <script>
    const list = ${JSON.stringify(list)};
    const el = document.getElementById('wc');
    function draw(){
      WordCloud(el, {
        list,
        gridSize: Math.round(Math.sqrt(el.clientWidth)/8),
        minSize: 6,    
        rotateRatio: 0,
        drawOutOfBound: false,
        backgroundColor: 'transparent'
      });
    }
    window.addEventListener('load', draw);
    window.addEventListener('resize', ()=>{ el.innerHTML=''; draw(); });
    </script>
    </body>
    </html>`;
      downloadBlob(`${filenameBase}.html`, new Blob([tpl], {type: 'text/html'}));
    }

  function refreshWordCloud() {
  // Use the last set the cloud was built from if available; otherwise derive from current filters
  const pubs = (_wc_lastPubs && _wc_lastPubs.length) ? _wc_lastPubs : (applyFilters().selectedPubs || []);
  renderWordCloud(pubs);
  }


    
  function bindEvents(){
  // Multi-select changes
  document.querySelectorAll('#filters select')
    .forEach(sel => sel.addEventListener('change', update));

  // Per-select Clear buttons
  document.querySelectorAll('.clear-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const id = btn.getAttribute('data-target');
      const el = document.getElementById(id);
      if (el && el.options) {
        Array.from(el.options).forEach(o => { o.selected = false; });
      }
      if (id === 'appointment') setDefaultAppointmentSelection();
      update();
    });
  });
  
  // Year inputs
  const yMin = document.getElementById('year-min');
  const yMax = document.getElementById('year-max');
  if (yMin) yMin.addEventListener('input', update);
  if (yMax) yMax.addEventListener('input', update);

  // Topic search
  const topic = document.getElementById('topic-search');
  if (topic) topic.addEventListener('input', debounce(update, 200));

  // Global reset
  const resetBtn = document.getElementById('reset-filters');
  if (resetBtn) {
    resetBtn.addEventListener('click', () => {
      document.querySelectorAll('#filters select').forEach(sel => {
        Array.from(sel.options).forEach(o => o.selected = false);
      });
      setDefaultAppointmentSelection();
      focusedAuthorID = null;
      focusedAuthorName = '';
      initYearInputs();
      if (topic) topic.value = '';
      activeTypes = new Set(DEFAULT_TYPE_SET);   // <—— add this line
      update();
    });
  }

    
// Keyboard shortcut: Shift+J => "Reset to defaults"
document.addEventListener('keydown', (e) => {
  const active = document.activeElement;
  const tag = (active?.tagName || '').toLowerCase();
  const inFormField = (active?.isContentEditable === true) || ['input','textarea','select'].includes(tag);
  if (inFormField) return;

  const key = (e.key || '').toLowerCase();
  if (e.shiftKey && !e.ctrlKey && !e.altKey && !e.metaKey && key === 'j') {
    e.preventDefault();
    if (resetBtn) resetBtn.click();
  }
});

// Accessibility & discoverability hints
if (resetBtn) {
  resetBtn.setAttribute('aria-keyshortcuts', 'Shift+J');
  if (!resetBtn.title) resetBtn.title = 'Reset to defaults (Shift+J)';
}
// Word Cloud: source toggle only
    document.querySelectorAll('input[name="wc-source"]').forEach(r => {
      r.addEventListener('change', () => renderWordCloud(_wc_lastPubs.length ? _wc_lastPubs : []));
    });

    if (!window.__wcResizeBound) {
      window.addEventListener('resize', debounce(() => {
        if (_wc_lastPubs.length) renderWordCloud(_wc_lastPubs);
      }, 200));
      window.__wcResizeBound = true;
    }

    // Reflow on resize (debounced)
    window.addEventListener('resize', debounce(() => {
      if (_wc_lastPubs.length) renderWordCloud(_wc_lastPubs);
    }, 200));

     
  // Export button: wire up click + set initial count
  const exportBtn = document.getElementById('export-selection');
  if (exportBtn) {
    exportBtn.addEventListener('click', () => exportCurrentSelectionCSV(lastSelectedPubs));
    setExportButtonCount(0);
  }
   // Jump to filters
   const jumpFiltersBtn = document.getElementById('jump-to-filters');
   if (jumpFiltersBtn) {
     jumpFiltersBtn.addEventListener('click', () => {
       document.getElementById('filters')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
     });
   }
    
  // === Exports: Publications bar chart ===
  document.getElementById('export-pub-chart-png')?.addEventListener('click', () =>
    exportPlotlyImage('pub-chart', 'png', 'publications_by_year'));
  document.getElementById('export-pub-chart-svg')?.addEventListener('click', () =>
    exportPlotlyImage('pub-chart', 'svg', 'publications_by_year'));
  document.getElementById('export-pub-chart-html')?.addEventListener('click', () =>
    exportPlotlyHTML('pub-chart', 'publications_by_year', 'Publications by Year'));
  
  // === Exports: Network ===
  document.getElementById('export-network-png')?.addEventListener('click', () =>
    exportPlotlyImage('coauthor-network', 'png', 'coauthorship_network'));
  document.getElementById('export-network-svg')?.addEventListener('click', () =>
    exportPlotlyImage('coauthor-network', 'svg', 'coauthorship_network'));
  document.getElementById('export-network-html')?.addEventListener('click', () =>
    exportPlotlyHTML('coauthor-network', 'coauthorship_network', 'Co-authorship Network'));
  
 
  // === Exports: Word Cloud ===

  document.getElementById('refresh-wordcloud')?.addEventListener('click', refreshWordCloud);
  document.getElementById('export-wordcloud-html')?.addEventListener('click', () =>
    exportWordCloudHTML('word_cloud'));
    
  // === Exports: International Map ===
  document.getElementById('export-intl-png')?.addEventListener('click', () =>
    exportPlotlyImage('intl-map', 'png', 'international_collaborations'));
  document.getElementById('export-intl-svg')?.addEventListener('click', () =>
    exportPlotlyImage('intl-map', 'svg', 'international_collaborations'));
  document.getElementById('export-intl-html')?.addEventListener('click', () =>
    exportPlotlyHTML('intl-map', 'international_collaborations', 'International Collaborations')); 


  // === Exports: Author–topic PCA ===
  document.getElementById('export-pca-png')?.addEventListener('click', () =>
    exportPlotlyImage('author-topic-pca', 'png', 'author_topic_pca'));
  document.getElementById('export-pca-svg')?.addEventListener('click', () =>
    exportPlotlyImage('author-topic-pca', 'svg', 'author_topic_pca'));
  document.getElementById('export-pca-html')?.addEventListener('click', () =>
    exportPlotlyHTML('author-topic-pca', 'author_topic_pca', 'Author–topic PCA'));

  // === Exports: Top partner institutions (ROR) ===
  document.getElementById('export-ror-png')?.addEventListener('click', () =>
    exportPlotlyImage('top-ror', 'png', 'top_partner_institutions'));
  document.getElementById('export-ror-svg')?.addEventListener('click', () =>
    exportPlotlyImage('top-ror', 'svg', 'top_partner_institutions'));
  document.getElementById('export-ror-html')?.addEventListener('click', () =>
    exportPlotlyHTML('top-ror', 'top_partner_institutions', 'Top partner institutions (ROR)'));

    
  }


    // ============ Filtering logic ============
    function applyFilters(){
      // Years
      const ymin = clampYear(document.getElementById('year-min').value);
      const ymax = clampYear(document.getElementById('year-max').value);
      yearBounds = { min: Math.min(ymin, ymax), max: Math.max(ymin, ymax) };
    
      // Topic query
      const qEl = document.getElementById('topic-search');
      const topicQ = (qEl && qEl.value) ? qEl.value.trim().toLowerCase() : '';
    
      // Roster attribute filters (ignored if focusing on one author)
      const selectedLevels = getMulti('level');
      const selectedCats   = getMulti('category');
      const selectedAppt   = getMulti('appointment');
      const selectedRGs    = getMulti('research-group');
    
      // Choose source:
      const sourcePubs = (focusedAuthorID && perAuthorData.length) ? perAuthorData : pubData;
    
      // Filter pubs by year + topic first
      let pubs = sourcePubs.filter(p => p.publication_year >= yearBounds.min && p.publication_year <= yearBounds.max);
      if (topicQ) pubs = pubs.filter(p => fuzzyQueryMatch(topicQ, p._topic_haystack));
    
      let contributingRoster;
    
      if (focusedAuthorID) {
        const fa = normalizeID(focusedAuthorID);
    
        if (perAuthorData && sourcePubs === perAuthorData) {
          // Per-author dataset: keep ONLY rows for the focused author
          pubs = pubs.filter(p => normalizeID(p.author_openalex_id) === fa);
        } else {
          // Dedup dataset fallback: keep works where the author appears anywhere
          const hasUnion = (p) => {
            const union = String(p.cohort_union_author_ids || '');
            return union && ('|' + union + '|').includes('|' + fa + '|');
          };
          const inAuthorships = (p) => {
            const ids = splitPipe(p['authorships__author__id']).map(normalizeID);
            return ids.includes(fa);
          };
          pubs = pubs.filter(p =>
            normalizeID(p.author_openalex_id) === fa || hasUnion(p) || inAuthorships(p)
          );
        }
    
        // Final safety (focused): deduplicate by work
        {
          const seen = new Set();
          pubs = pubs.filter(p => {
            const k = workKey(p);
            if (seen.has(k)) return false;
            seen.add(k);
            return true;
          });
        }
    
        // Focused = just that one roster entry
        contributingRoster = rosterData.filter(r => normalizeID(r.OpenAlexID) === fa);
    
      } else {
        // Not focused: apply roster attribute filters first
        const filteredRoster = rosterData.filter(r => {
          if (selectedLevels.length && !selectedLevels.includes(r.Level || '')) return false;
          if (selectedCats.length   && !selectedCats.includes(r.Category || '')) return false;
          if (selectedAppt.length   && !selectedAppt.includes(r.Appointment || '')) return false;
          if (selectedRGs.length    && !r._RGs.some(g => selectedRGs.includes(g))) return false;
          return true;
        });
    
        // Then limit pubs to those whose representative author is in filteredRoster
        const allowedIDs = new Set(filteredRoster.map(r => normalizeID(r.OpenAlexID)));
        pubs = pubs.filter(p => allowedIDs.has(normalizeID(p.author_openalex_id)));
    
        // Contributing roster = filtered members who actually have pubs after all filters
        const havePubIDs = new Set(pubs.map(p => normalizeID(p.author_openalex_id)));
        contributingRoster = filteredRoster.filter(r => havePubIDs.has(normalizeID(r.OpenAlexID)));
      }
    
      return { contributingRoster, selectedPubs: pubs };
    }


    function getMulti(id){
      const el = document.getElementById(id);
      return Array.from(el.selectedOptions).map(o => o.value);
    }

    let _wc_lastPubs = [];  // remember last set for resize reflow
    
    const renderWordCloud = debounce(function(pubs){
      const container = document.getElementById('wordcloud');
      if (!container) return;
    
      // If the bar chart is present, honor which types are currently visible in the legend
      let filtered = pubs;
      try {
        const el = document.getElementById('pub-chart');
        const visibleTypes = new Set(
          (el && el.data ? el.data : [])
            .filter(tr => tr.visible === true || tr.visible === undefined)  // visible traces
            .map(tr => String(tr.name).toLowerCase())
        );
        if (visibleTypes.size) {
          filtered = pubs.filter(p => visibleTypes.has(String((p.type || 'other')).toLowerCase()));
        }
      } catch (_) { /* non-fatal if chart not initialized yet */ }
    
      const source = document.querySelector('input[name="wc-source"]:checked')?.value || 'concepts';
      const list = wc_buildList(filtered, source, 100);  // fixed to 100
    
      container.innerHTML = ''; // clear
      if (!list.length) {
        container.innerHTML = '<div class="muted" style="padding:10px">No terms in current selection.</div>';
        return;
      }
    
      // Size scaling: keep largest readable but not overwhelming
      const maxW = list[0][1] || 1;
      const area = container.clientWidth * container.clientHeight;
      const base = Math.max(12, Math.min(48, Math.sqrt(area) / 12));
      const weightFactor = (w) => base + (w / maxW) * (base * 1.8);
    
      WordCloud(container, {
        list,
        gridSize: Math.round(Math.sqrt(container.clientWidth) / 8),
        weightFactor,
        rotateRatio: 0,              // NO rotation
        drawOutOfBound: false,
        backgroundColor: 'transparent',
      
        click: (item) => {
          const term = item && item[0];
          if (!term) return;
      
          const mode = document.querySelector('input[name="wc-mode"]:checked')?.value || 'local';
          const toAdd = (/\s/.test(term)) ? `"${term}"` : term;
      
          if (mode === 'local') {
            // Zoom in: append term; keep all filters
            const input = document.getElementById('topic-search');
            if (!input) return;
            const val = (input.value || '').trim();
            input.value = val ? (val + ' ' + toAdd) : toAdd;
            update();
            return;
          }
      
          // Global Search:
          // - Replace topic query with ONLY the clicked term
          // - RESET years to defaults
          // - CLEAR selected faculty (focused author)
          // - KEEP all other filters (RG/Level/Category/Appointment) and legend/type visibility
          try {
            const input = document.getElementById('topic-search');
            if (input) input.value = toAdd;
      
            // Reset years only
            if (typeof initYearInputs === 'function') initYearInputs();
      
            // Reset focused author (selected faculty)
            if (typeof clearFocusedAuthor === 'function') {
              clearFocusedAuthor(); // if you have a helper that also syncs UI
            } else {
              // fall back to clearing the focus variables
              if (typeof focusedAuthorID !== 'undefined') focusedAuthorID = null;
              if (typeof focusedAuthorName !== 'undefined') focusedAuthorName = '';
            }
      
            // Do NOT clear RG/Level/Category/Appointment selects
            // Do NOT reset activeTypes unless you explicitly want to
            update();
            return;
          } catch {
            // Fallback: behave like local zoom
            const input = document.getElementById('topic-search');
            if (!input) return;
            const val = (input.value || '').trim();
            input.value = val ? (val + ' ' + toAdd) : toAdd;
            update();
          }
        },
      
        hover: (item) => {
          container.title = item ? `${item[0]} — ${item[1]} pubs` : '';
        }
      });
      _wc_lastPubs = pubs.slice();
    }, 150);

// ===== Author–topic PCA (Jaccard via classical MDS) =======================

// Collect topic tokens for each contributing author from selected pubs.
// Uses OpenAlex Topics by default; falls back to concepts when topics missing.
function buildAuthorTopicSets(contributingRoster, selectedPubs) {
  const cohort = (contributingRoster || []).map(r => ({
    id: String(r.OpenAlexID || '').replace(/^https?:\/\/openalex\.org\/authors\//i, ''),
    name: r.Name || r.DisplayName || r.name || '(unknown)'
  }));
  const byAuthor = new Map(cohort.map(a => [a.id, { name: a.name, terms: new Set() }]));

  const getTerms = (p) => {
    // Prefer Topics (primary topic + subfield); else concepts_list
    const t1 = (p.primary_topic__display_name || '').trim().toLowerCase();
    const t2 = (p.primary_topic__subfield__display_name || '').trim().toLowerCase();
    let terms = [t1, t2].filter(Boolean);
    if (!terms.length) {
      const raw = p.concepts_list || '';
      terms = raw.split(/[\|;,]/).map(s => s.trim().toLowerCase()).filter(Boolean);
    }
    // normalize: keep simple tokens; drop 1-char fragments
    return Array.from(new Set(terms.map(s => s.replace(/\s+/g, ' ').trim())))
      .filter(s => s.length > 1);
  };

  // index-aligned authorship arrays from your CSV columns
  const parseAuthorships = (pub) => {
    const ids = (String(pub['authorships__author__id'] || '')
      .split('|').map(s => s.replace(/^https?:\/\/openalex\.org\/authors\//i, '').trim()));
    return ids;
  };

  // Fill sets
  for (const p of (selectedPubs || [])) {
    const terms = getTerms(p);
    if (!terms.length) continue;
    const ids = parseAuthorships(p);
    for (const id of ids) {
      if (!id || !byAuthor.has(id)) continue; // only cohort authors
      const rec = byAuthor.get(id);
      terms.forEach(t => rec.terms.add(t));
    }
  }

  // Keep only authors who have at least 1 term in the current selection
  const authors = [];
  for (const [id, { name, terms }] of byAuthor.entries()) {
    if (terms.size) authors.push({ id, name, terms });
  }
  return authors;
}

  // Jaccard distance between two Sets
  function jaccardDistance(A, B) {
    if (!A.size && !B.size) return 0;
    let inter = 0;
    for (const x of A) if (B.has(x)) inter++;
    const union = A.size + B.size - inter;
    return union ? 1 - (inter / union) : 1;
  }
  
  // Classical MDS to 2D from distance matrix D (NxN)
  // Returns { x:[], y:[], eigenvalues:[] }
  function classicalMDS2D(D) {
    const N = D.length;
    if (N < 2) return { x: [], y: [], eigenvalues: [] };
  
    // Build B = -0.5 * J * (D^2) * J, where J = I - 11^T/N
    const J = (i, j) => (i === j ? 1 - 1/N : -1/N);
    const DSq = Array.from({ length: N }, (_, i) =>
      Array.from({ length: N }, (_, j) => D[i][j] * D[i][j])
    );
  
    // Center rows/cols: B = -0.5 * J * DSq * J
    const B = Array.from({ length: N }, () => Array(N).fill(0));
    // Compute M = DSq * J
    const M = Array.from({ length: N }, () => Array(N).fill(0));
    for (let i = 0; i < N; i++) {
      for (let j = 0; j < N; j++) {
        let s = 0;
        for (let k = 0; k < N; k++) s += DSq[i][k] * J(k, j);
        M[i][j] = s;
      }
    }
    // B = -0.5 * J * M
    for (let i = 0; i < N; i++) {
      for (let j = 0; j < N; j++) {
        let s = 0;
        for (let k = 0; k < N; k++) s += J(i, k) * M[k][j];
        B[i][j] = -0.5 * s;
      }
    }
  
    // Power iteration for top 2 eigenpairs of symmetric B (deflation)
    function powerIter(A, iters = 150) {
      const n = A.length;
      let v = Array.from({ length: n }, () => Math.random());
      // normalize
      const norm = (x) => Math.sqrt(x.reduce((a,b)=>a+b*b,0)) || 1;
      let nv = norm(v); v = v.map(x => x/nv);
      let lambda = 0;
      for (let t = 0; t < iters; t++) {
        const Av = Array.from({ length: n }, (_, i) => {
          let s = 0; for (let j = 0; j < n; j++) s += A[i][j] * v[j];
          return s;
        });
        lambda = v.reduce((s,vi,i)=> s + vi*Av[i], 0); // Rayleigh
        nv = norm(Av);
        v = Av.map(x => x / (nv || 1));
      }
      return { value: lambda, vector: v };
    }
  
    // 1st eigen
    const e1 = powerIter(B);
    // Deflate
    const n = B.length;
    const B2 = Array.from({ length: n }, (_, i) =>
      Array.from({ length: n }, (_, j) => B[i][j] - e1.value * e1.vector[i] * e1.vector[j])
    );
    // 2nd eigen
    const e2 = powerIter(B2);
  
    const lam1 = Math.max(e1.value, 0);
    const lam2 = Math.max(e2.value, 0);
    const x = e1.vector.map(v => Math.sqrt(lam1) * v);
    const y = e2.vector.map(v => Math.sqrt(lam2) * v);
    return { x, y, eigenvalues: [lam1, lam2] };
  }
  
  function drawAuthorTopicPCA(selectedPubs, contributingRoster) {
    const container = document.getElementById('author-topic-pca');
    const meta = document.getElementById('pca-meta');
    if (!container) return;
    const _term = (document.getElementById('pca-search')?.value || '').trim().toLowerCase();
    const hasQuery = _term.length > 0;

    const authors = buildAuthorTopicSets(contributingRoster, selectedPubs);
    if (!authors.length || authors.length < 2) {
      container.innerHTML = '';
      if (meta) meta.textContent = 'Not enough data in current selection to compute PCA.';
      return;
    }
  
    // Distance matrix (Jaccard)
    const N = authors.length;
    const D = Array.from({ length: N }, () => Array(N).fill(0));
    for (let i = 0; i < N; i++) for (let j = i+1; j < N; j++) {
      const d = jaccardDistance(authors[i].terms, authors[j].terms);
      D[i][j] = D[j][i] = d;
    }
  
    // 2D embedding
    const { x, y, eigenvalues } = classicalMDS2D(D);
  
    // Build per-author metadata
    const points = authors.map((a, i) => {
      const { groups, isFull } = authorMetaFromRoster(a.id, contributingRoster);
      const group = groups[0] || 'Unassigned';
      const topicSample = Array.from(a.terms).slice(0, 6).join(', ');
      return {
        i, x: x[i], y: y[i],
        name: a.name,
        group,
        isFull,
        hit: hasQuery ? stripDiacritics(a.name).toLowerCase().includes(_term) : true,  // treat all as "hit" if no query
        hover: `${a.name}<br><span style="color:#666">Topics: ${topicSample || '—'}</span>`
      };
    });
  
    // Partition into traces by group for a clean legend and colors
    const groups = Array.from(new Set(points.map(p => p.group)));
    const palette = [
      '#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd',
      '#8c564b','#e377c2','#7f7f7f','#bcbd22','#17becf'
    ];
    const colorOf = (g) => palette[groups.indexOf(g) % palette.length];
  
    const traces = groups.map(g => {
      const pts = points.filter(p => p.group === g);
    
      const xs = pts.map(p => p.x);
      const ys = pts.map(p => p.y);
      const labels = pts.map(p => p.name);
    
      // Style arrays per point
      const sizes   = pts.map(p => p.hit ? 16 : 10);
      const opac    = pts.map(p => p.hit ? 1.0 : 0.6);
      const lw      = pts.map(p => (p.hit ? 2.5 : (p.isFull ? 1.5 : 0)));
      const lcolor  = pts.map(_ => '#000');
    
      return {
        name: g,
        type: 'scatter',
        mode: 'markers',
        x: xs,
        y: ys,
        text: labels,
        hovertemplate: '%{text}<br>x: %{x:.2f}<br>y: %{y:.2f}<extra></extra>',
        marker: {
          size: sizes,
          opacity: opac,
          color: colorOf(g),
          line: { width: lw, color: lcolor }
        }
      };
    });
    const layout = {
      margin: { t: 20, r: 10, b: 60, l: 50 },
      height: 320,
      legend: {
        orientation: 'h',
        x: 0, xanchor: 'left',
        y: 1.12, yanchor: 'bottom'
      },
      xaxis: {
        zeroline: false,
        title: 'PC1 (from distances)',
        automargin: true,
        title_standoff: 10
      },
      yaxis: {
        zeroline: false,
        title: 'PC2 (from distances)',
        automargin: true
      },
      // ✅ ensure hover compares both axes / nearest point only
      hovermode: 'closest',
      hoverdistance: -1,     // nearest only
      spikedistance: -1
    };

    Plotly.newPlot(container, traces, layout, { responsive: true });
    
    // Cache labels per trace so highlighting can restyle without recomputing PCA
    window.__pcaState = {
      containerId: 'author-topic-pca',
      labelsByTrace: (traces || []).map(tr => (tr.text || []).slice())  // clone labels
    };
    
    // Apply current search term purely as a style update (no redraw)
    if (typeof pcaHighlightByName === 'function') {
      const term = document.getElementById('pca-search')?.value || '';
      pcaHighlightByName(term);
    }

    if (meta) {
      const ev = (eigenvalues || []).map(v => v.toFixed(2)).join(', ');
      meta.textContent = `Authors: ${authors.length}. Eigenvalues (top-2): [${ev}] — lower distance = closer topical overlap.`;
    }
  }

  function drawTopRORPartners(contributingRoster, selectedPubs) {
    const container = document.getElementById('top-ror');
    const meta = document.getElementById('ror-meta');
    if (!container) return;
  
    // Build cohort OpenAlex ID set (normalized)
    const cohortIDs = new Set(
      (contributingRoster || []).map(r =>
        String(r.OpenAlexID || '').replace(/^https?:\/\/openalex\.org\/authors\//i, '')
      )
    );
    if (!cohortIDs.size) {
      container.innerHTML = '';
      if (meta) meta.textContent = 'No selected cohort authors.';
      return;
    }
  
    // --- NEW: infer the cohort's "home" ROR globally (mode across cohort authorships)
    // Uses parseAuthorships() which already aligns and normalizes institution RORs.
    const talliesHome = new Map(); // ror -> count
    for (const p of (selectedPubs || [])) {
      const a = parseAuthorships(p); // { ids, instRor, instName, instCountry3, ... }
      const n = a.ids.length;
      for (let i = 0; i < n; i++) {
        const id = a.ids[i];
        const ror = (a.instRor[i] || '').trim();
        if (!id || !ror) continue;
        if (cohortIDs.has(id)) {
          talliesHome.set(ror, (talliesHome.get(ror) || 0) + 1);
        }
      }
    }
    let homeROR = '';
    {
      let bestN = 0;
      for (const [k, n] of talliesHome.entries()) {
        if (n > bestN) { homeROR = k; bestN = n; }
      }
    }
    // --- END NEW
  
    // Tally partner credits: unique partner ROR per cohort author per paper.
    const tally = new Map(); // ror -> { name, country, n }
    let paperCredits = 0;
  
    for (const p of (selectedPubs || [])) {
      const a = parseAuthorships(p); // aligned arrays
      const n = a.ids.length;
      if (!n) continue;
  
      // Track which partner RORs we credited for this paper across all cohort authors
      const creditedThisCohort = new Set();
  
      // For every cohort author on this paper…
      for (let i = 0; i < n; i++) {
        const id = a.ids[i];
        if (!cohortIDs.has(id)) continue;
  
        // Cohort author's "own" institution ROR on this paper (first inst per authorship)
        const ownRor = (a.instRor[i] || '').trim();
  
        // Scan all coauthors' first-institution RORs
        for (let j = 0; j < n; j++) {
          if (j === i) continue; // skip self
          const ror = (a.instRor[j] || '').trim();
          if (!ror) continue;
  
          // If we know the cohort author's own ROR, skip that (existing rule)
          if (ownRor && ror === ownRor) continue;
  
          // --- NEW: always skip the inferred cohort-wide home ROR (even if ownRor was missing)
          if (homeROR && ror === homeROR) continue;
  
          creditedThisCohort.add(ror);
        }
      }
  
      // Credit each unique partner ROR at most once per paper (across all cohort authors)
      if (creditedThisCohort.size) {
        paperCredits++;
        for (const ror of creditedThisCohort) {
          const idx = a.instRor.findIndex(x => x === ror);
          const name = (idx >= 0 ? a.instName[idx] : '') || '';
          const country = (idx >= 0 ? a.instCountry3[idx] : '') || '';
          const rec = tally.get(ror) || { name: name || ror, country, n: 0 };
          if (!rec.name && name) rec.name = name;         // fill missing metadata if seen later
          if (!rec.country && country) rec.country = country;
          rec.n += 1;
          tally.set(ror, rec);
        }
      }
    }
  
    // Build rows, exclude the home ROR again as a safety net
    let rows = Array.from(tally.entries())
      .filter(([ror]) => !homeROR || ror !== homeROR) // NEW safety filter
      .map(([ror, rec]) => ({
        ror,
        name: rec.name || ror,
        country: rec.country || '',
        n: rec.n
      }))
      .sort((a, b) => b.n - a.n)
      .slice(0, 12);
  
    container.innerHTML = '';
    if (!rows.length) {
      if (meta) meta.textContent =
        'No partner institutions found (after excluding cohort authors’ own institutions and cohort home institution).';
      return;
    }
  
    // Plotly horizontal bar
    const yLabels = rows.map(r => r.name);
    const xVals = rows.map(r => r.n);
  
    const hover = rows.map(r => {
      let s = `${r.name}<br>ROR: ${r.ror}`;
      if (r.country) s += `<br>Country: ${r.country}`;
      s += `<br>Papers: ${r.n}`;
      return s;
    });
  
    // Simple palette by country (stable mapping by first occurrence)
    const countries = Array.from(new Set(rows.map(r => r.country)));
    const palette = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2','#7f7f7f','#bcbd22','#17becf'];
    const colorOf = (c) => palette[countries.indexOf(c) % palette.length];
    const barColors = rows.map(r => colorOf(r.country));
  
    const trace = {
      type: 'bar',
      orientation: 'h',
      x: xVals,
      y: yLabels,
      hovertext: hover,
      hovertemplate: '%{hovertext}<extra></extra>',
      marker: { color: barColors }
    };
  
    const layout = {
      margin: { t: 18, r: 10, b: 40, l: 160 },
      height: 320,
      xaxis: { title: 'Papers (unique partner credit)' },
      yaxis: { automargin: true },
      showlegend: false
    };
  
    Plotly.newPlot(container, [trace], layout, { responsive: true });
  
    if (meta) {
      const topK = rows.length;
      const excl = homeROR ? ' (excluding cohort home institution)' : '';
      meta.textContent =
        `Showing top ${topK} partner RORs${excl}; credits counted on ${paperCredits} papers in current selection.`;
    }
  }

  function pcaHighlightByName(rawTerm) {
  const el = document.getElementById('author-topic-pca');
  const st = window.__pcaState;
  if (!el || !st || !Array.isArray(st.labelsByTrace) || !el.data) return;

  const term = String(rawTerm || '').trim().toLowerCase();
  const has = term.length > 0;
  const norm = (s) => (typeof stripDiacritics === 'function' ? stripDiacritics(String(s)) : String(s)).toLowerCase();

  st.labelsByTrace.forEach((labels, traceIndex) => {
    const sizes = labels.map(name => has && norm(name).includes(term) ? 16 : 10);
    const opac  = labels.map(name => has && norm(name).includes(term) ? 1.0 : 0.6);
    const lw    = labels.map(name => has && norm(name).includes(term) ? 2.5 : 0);

    Plotly.restyle(el, {
      'marker.size': [sizes],
      'marker.opacity': [opac],
      'marker.line.width': [lw]
    }, [traceIndex]);
  });
}


    // ============ Rendering ============
   
// === Publications header helpers =========================================
function ensurePublicationsHeaderUI() {
  const panel = document.getElementById('publications-panel');
  if (!panel) return { countEl: null, filtersEl: null };

  const head = panel.querySelector('.panel-head');
  if (!head) return { countEl: null, filtersEl: null };

  // 1) Inline count next to "Publications"
  let title = head.querySelector('h2');
  if (!title) {
    title = document.createElement('h2');
    head.prepend(title);
  }
  // Ensure a span for a live count
  let countEl = head.querySelector('#publications-count');
  if (!countEl) {
    countEl = document.createElement('span');
    countEl.id = 'publications-count';
    countEl.style.marginLeft = '8px';
    countEl.className = 'muted';
    title.appendChild(countEl);
  }

  // 2) Active filters line (shows chips)
  let filtersEl = panel.querySelector('#active-filters');
  if (!filtersEl) {
    filtersEl = document.createElement('div');
    filtersEl.id = 'active-filters';
    // Place it just under the panel head, above the list
    head.insertAdjacentElement('afterend', filtersEl);
  }

  return { countEl, filtersEl };
}
function ensurePcaSearchUI(){
  const chartEl = document.getElementById('author-topic-pca');
  if (!chartEl) return;

  // If tools bar already exists, don’t add again
  if (document.getElementById('pca-tools')) return;

  const tools = document.createElement('div');
  tools.id = 'pca-tools';
  tools.style.display = 'flex';
  tools.style.alignItems = 'center';
  tools.style.gap = '8px';
  tools.style.margin = '6px 0 8px 0';

  tools.innerHTML = `
    <label for="pca-search" style="font-size:0.9rem" title="Type part of a first/last name">
      Highlight author:
    </label>
    <input id="pca-search" type="text" placeholder="e.g., Smith or Maria"
           style="max-width:260px;padding:6px 8px;border:1px solid #ccc;border-radius:6px">
    <button id="pca-clear" type="button"
            style="padding:6px 10px;border:1px solid #ccc;border-radius:6px;background:#f7f7f7;cursor:pointer">
      Clear
    </button>
  `;
  chartEl.insertAdjacentElement('beforebegin', tools);

  // AFTER: restyle-only, no recompute, no redraw
  const onInput = debounce(() => {
    const val = document.getElementById('pca-search')?.value || '';
    pcaHighlightByName(val);
  }, 80);
  
  document.getElementById('pca-search')?.addEventListener('input', onInput);
  document.getElementById('pca-clear')?.addEventListener('click', () => {
    const inp = document.getElementById('pca-search');
    if (inp) inp.value = '';
    pcaHighlightByName('');
  });
}

function summarizeMulti(values, maxShown = 4) {
  const list = (values || []).filter(Boolean);
  if (!list.length) return '';
  if (list.length <= maxShown) return list.join(', ');
  const shown = list.slice(0, maxShown).join(', ');
  return `${shown} +${list.length - maxShown} more`;
}

function getActiveTypeLabelsFromLegend() {
  // Mirror the bar chart legend state (normalized, lower-case names)
  const el = document.getElementById('pub-chart');
  if (!el || !el.data) return [];
  const on = (tr) => (tr.visible === true || tr.visible === undefined);
  return el.data.filter(on).map(tr => String(tr.name).toLowerCase());
}

/**
 * Build & render the chips that reflect current filters.
 * - topic query
 * - focused author
 * - Level / Category / Appointment
 * - Research Group(s)
 * - Year range (when narrowed)
 * - Visible publication types (legend)
 */
function renderActiveFilters(contributingRoster, selectedPubs) {
  const { countEl, filtersEl } = ensurePublicationsHeaderUI();
  if (!filtersEl) return;

  // Count (n=...) is set by setExportButtonCount() and here for header too
  if (countEl && Array.isArray(lastSelectedPubs)) {
    countEl.textContent = ` (n=${lastSelectedPubs.length})`;
  }

  const chips = [];

  // Topic
  const qEl = document.getElementById('topic-search');
  const topicQ = (qEl && qEl.value) ? qEl.value.trim() : '';
  if (topicQ) chips.push({ label: 'Topic', value: topicQ });

  // Focused author
  if (typeof focusedAuthorID !== 'undefined' && focusedAuthorID && typeof focusedAuthorName !== 'undefined' && focusedAuthorName) {
    chips.push({ label: 'Author', value: focusedAuthorName });
  } else {
    // Roster attribute filters
    const getMulti = (id) => Array.from((document.getElementById(id)?.selectedOptions || []))
                                  .map(o => o.value).filter(Boolean);

    const levels = getMulti('level');
    if (levels.length) chips.push({ label: 'Level', value: summarizeMulti(levels) });

    const cats = getMulti('category');
    if (cats.length) chips.push({ label: 'Category', value: summarizeMulti(cats) });

    const appt = getMulti('appointment');
    if (appt.length) chips.push({ label: 'Appointment', value: summarizeMulti(appt) });

    const rgs = getMulti('research-group');
    if (rgs.length) chips.push({ label: 'Research group', value: summarizeMulti(rgs) });
  }

  // Years (only show if not the full default span)
  const ymin = clampYear(document.getElementById('year-min')?.value);
  const ymax = clampYear(document.getElementById('year-max')?.value);
  if (typeof DEFAULT_START_YEAR !== 'undefined' && typeof DEFAULT_END_YEAR !== 'undefined') {
    if (ymin !== DEFAULT_START_YEAR || ymax !== DEFAULT_END_YEAR) {
      chips.push({ label: 'Years', value: `${Math.min(ymin, ymax)}–${Math.max(ymin, ymax)}` });
    }
  } else {
    // fallback if constants not present
    chips.push({ label: 'Years', value: `${Math.min(ymin, ymax)}–${Math.max(ymin, ymax)}` });
  }

  // Publication types (from legend)
  try {
    const visTypes = getActiveTypeLabelsFromLegend();
    if (visTypes.length) {
      chips.push({ label: 'Types', value: summarizeMulti(visTypes.map(s => s.replaceAll('-', ' '))) });
    }
  } catch (_) {}

  // Render chips
  if (!chips.length) {
    filtersEl.innerHTML = '<div class="muted">No active filters.</div>';
    return;
  }

  filtersEl.innerHTML = chips.map(c => `
    <span class="chip" title="${escapeHTML(c.label)}: ${escapeHTML(c.value)}">
      <strong class="mono">${escapeHTML(c.label)}:</strong>&nbsp;${escapeHTML(c.value)}
    </span>
  `).join(' ');
}

function update(){
      const { contributingRoster, selectedPubs } = applyFilters();
    
      // Ensure we have a default active type set
      if (!activeTypes) activeTypes = new Set(DEFAULT_TYPE_SET);
    
      // Apply type filter ONLY to the Publications panel (as requested)
      const pubsForList = selectedPubs.filter(p => activeTypes.has(p._type_norm || 'other'));
    
      // Keep export in sync with the visible list
      lastSelectedPubs = pubsForList.slice();
      setExportButtonCount(pubsForList.length);
    
      // Draw chart from ALL selectedPubs (legend controls visibility)
      drawBarChart(selectedPubs);
    
      renderWordCloud(selectedPubs);
    
      // Other panels unchanged
      drawFacultyTable(contributingRoster);
      drawPublicationList(pubsForList);
      renderActiveFilters(contributingRoster, selectedPubs);

      updateCoauthorPanels(contributingRoster, selectedPubs);
    
      // === INTERNATIONAL MAP WIRING ===
      window.__currentSelectedPubs = selectedPubs;
      window.__contributingRoster  = contributingRoster;
      
      // Determine cohort home country (ISO-3)
      const homeISO3 = computeHomeCountry(contributingRoster, selectedPubs);
      window.__homeISO3 = homeISO3;

      // Position-aware series per your FL/M rules
      const intlSeries = computeIntlLinks(selectedPubs, contributingRoster, homeISO3);

      // Render choropleth with home highlighted in green
      drawInternationalMap(intlSeries, homeISO3);
      // === END INTERNATIONAL MAP WIRING ===
    
      const fc = document.getElementById('faculty-count');
      if (fc) {
        const base = `Faculty contributing: ${contributingRoster.length}`;
        fc.textContent = focusedAuthorID ? `${base} (Focused: ${focusedAuthorName}. Use Reset to clear)` : base;
      }
      // after renderActiveFilters(contributingRoster, selectedPubs);
      drawAuthorTopicPCA(selectedPubs, contributingRoster);
      drawTopRORPartners(contributingRoster, selectedPubs);

    }



    function drawBarChart(pubs){
      // Count by year x normalized type
      const counts = new Map(); // key: `${year}::${type}` -> count
      const years = new Set();
      const types = new Set();
    
      pubs.forEach(p => {
        const y = p.publication_year;
        const t = (p._type_norm || 'other');
        years.add(y); types.add(t);
        const k = `${y}::${t}`;
        counts.set(k, (counts.get(k) || 0) + 1);
      });
    
      const sortedYears = Array.from(years).sort((a,b)=>a-b);
      const sortedTypes = Array.from(types).sort((a,b)=>a.localeCompare(b)); // A→Z
    
      // Build Plotly series (stacked bars), with default visibility via activeTypes
      const traces = sortedTypes.map(t => {
        const yvals = sortedYears.map(y => counts.get(`${y}::${t}`) || 0);
        // Honor activeTypes for legend default/restore
        const vis = (activeTypes && !activeTypes.has(t)) ? 'legendonly' : true;
        return {
          x: sortedYears,
          y: yvals,
          name: t,                // legend label (normalized)
          type: 'bar',
          visible: vis
        };
      });
    
      const layout = {
        barmode: 'stack',
        xaxis: { title: 'Year', dtick: 1, range: [yearBounds.min - 0.5, yearBounds.max + 0.5] },
        yaxis: { title: 'Publications' },
        margin: { t: 20, r: 10, b: 40, l: 50 },
        height: 300,
        legend: { traceorder: 'normal' } // keep A→Z as built
      };
    
      const el = document.getElementById('pub-chart');
      Plotly.react(el, traces, layout, {displayModeBar:false});
    
      // Bind legend interactions once so they drive the Publications panel
      if (!el.__legendBound && typeof el.on === 'function') {
        // Single-click: toggle just that type
        el.on('plotly_legendclick', (ev) => {
          try {
            const idx = ev.curveNumber;
            const t  = String(el.data?.[idx]?.name || '').toLowerCase();
            if (!t) return false;
            if (!activeTypes) activeTypes = new Set(DEFAULT_TYPE_SET);
            if (activeTypes.has(t)) activeTypes.delete(t); else activeTypes.add(t);
            update();
          } catch(_) {}
          return false; // prevent Plotly's default toggle (we re-render explicitly)
        });
    
        // Double-click on legend: isolate that one type (classic UX)
        el.on('plotly_legenddoubleclick', (ev) => {
          try {
            const idx = ev.curveNumber;
            const t  = String(el.data?.[idx]?.name || '').toLowerCase();
            if (!t) return false;
            activeTypes = new Set([t]);
            update();
          } catch(_) {}
          return false; // prevent Plotly's default isolate; we re-render
        });
    
        el.__legendBound = true;
      }
    }

    function drawInternationalMap(series, homeISO3){
      const el = document.getElementById('intl-map');
      if (!el) return;
    
      // Main trace = foreign countries with counts
      const mainTrace = {
        type: 'choropleth',
        locationmode: 'ISO-3',
        locations: series.iso3,                 // excludes home
        z: series.values,
        customdata: series.iso3.map(countryName),
        hovertemplate: '%{customdata}<br>Links: %{z}<extra></extra>',
        colorbar: { title: 'Links' },
        showscale: true
      };
    
      // Home trace = single country, solid green
      const homeTrace = (homeISO3 ? {
        type: 'choropleth',
        locationmode: 'ISO-3',
        locations: [homeISO3],
        z: [1],                                  // any constant
        customdata: [countryName(homeISO3)],
        hovertemplate: '%{customdata}<br>(home country)<extra></extra>',
        colorscale: [[0,'#22c55e'],[1,'#22c55e']], // solid green
        showscale: false                         // no second colorbar
      } : null);
    
      const traces = homeTrace ? [mainTrace, homeTrace] : [mainTrace];
    
      const layout = {
        margin: { t:10, r:10, b:10, l:10 },
        geo: { projection: { type: 'natural earth' } },
        height: 320
      };
    
      Plotly.react(el, traces, layout, { displayModeBar:false });
    
      const meta = document.getElementById('intl-meta');
      if (meta) {
        // Show foreign counts; also indicate home explicitly
        const homeLabel = homeISO3 ? ` · Home: ${countryName(homeISO3)}` : '';
        meta.textContent = `${series.iso3.length} countries · ${series.total} links${homeLabel}`;
      }
    
      // Click to open details (works for both traces)
      el.on('plotly_click', ev => {
        const pt = ev?.points?.[0];
        const iso3 = pt?.location;
        if (!iso3) return;
        showCountryDetail(iso3);
        const det = document.getElementById('intl-detail');
        if (det && !det.open) det.open = true;
      });
    }


    function showCountryDetail(iso3){
      const body = document.getElementById('intl-detail-body');
      if (!body) return;
    
      const pubs = (window.__currentSelectedPubs || []);
      const roster = (window.__contributingRoster || []);
      const homeISO3 = window.__homeISO3 || '';
    
      const cohortIDs = new Set(
        roster.map(r => String(r.OpenAlexID || '')
          .replace(/^https?:\/\/openalex\.org\/authors\//i,''))
      );
    
      // Helper: does this paper generate a link to iso3 per FL/M rules?
      function paperGeneratesLinkToCountry(p) {
        const a = parseAuthorships(p);
        const n = a.ids.length;
        if (!n) return { ok:false, a:null, cohortInvolved:[] };
    
        const cohortIdx_FL = [];
        const cohortIdx_M  = [];
        const intlIdx_any  = [];
        const intlIdx_FL   = [];
        for (let i=0;i<n;i++){
          const id  = a.ids[i];
          const pos = String(a.position[i] || '').toLowerCase();
          const cty = a.countries[i];
          const isCohort = id && cohortIDs.has(id);
          const isIntl   = cty && homeISO3 && cty !== homeISO3;
          const isFL     = (pos === 'first' || pos === 'last');
    
          if (isCohort && isFL) cohortIdx_FL.push(i);
          else if (isCohort && pos === 'middle') cohortIdx_M.push(i);
    
          if (isIntl && cty === iso3) {
            // Only track intl indices for the clicked country
            intlIdx_any.push(i);
            if (isFL) intlIdx_FL.push(i);
          }
        }
    
        // Rule 1
        if (cohortIdx_FL.length && intlIdx_any.length) {
          return { ok:true, a, cohortInvolved:[...cohortIdx_FL] };
        }
        // Rule 2
        if (cohortIdx_M.length && intlIdx_FL.length) {
          return { ok:true, a, cohortInvolved:[...cohortIdx_M] };
        }
        return { ok:false, a:null, cohortInvolved:[] };
      }
    
      const matched = [];
      for (const p of pubs) {
        const m = paperGeneratesLinkToCountry(p);
        if (m.ok) matched.push({ pub: p, a: m.a, cohortIdx: m.cohortInvolved });
      }
    
      if (!matched.length) {
        body.innerHTML = `<div class="muted">No qualifying publications for ${countryName(iso3)} under the FL/M rules.</div>`;
        return;
      }
    
      // Aggregate cohort authors who actually contributed to a qualifying link
      const authorHitCounts = new Map(); // display_name -> count
      matched.forEach(({a, cohortIdx}) => {
        const seen = new Set();
        cohortIdx.forEach(i => {
          const name = a.names[i] || a.ids[i];
          if (!seen.has(name)) {
            seen.add(name);
            authorHitCounts.set(name, (authorHitCounts.get(name) || 0) + 1);
          }
        });
      });
    
      const authorList = Array.from(authorHitCounts.entries())
        .sort((A,B) => (B[1]-A[1]) || A[0].localeCompare(B[0]))
        .map(([nm,c]) => `<li>${nm} <span class="muted">(${c})</span></li>`)
        .join('');
    
      const pubList = matched.slice(0,30).map(({pub}) => {
        const y   = pub.publication_year || pub.year || '';
        const t   = pub.title || pub.display_name || '(untitled)';
        const doi = (pub.doi && /^https?:\/\//i.test(pub.doi)) ? pub.doi : '';
        const doiLink = doi ? ` <a href="${doi}" target="_blank" rel="noopener">DOI</a>` : '';
        return `<li>${y} — ${t}${doiLink}</li>`;
      }).join('');
    
      body.innerHTML = `
        <div style="margin-bottom:8px">
          <strong>${countryName(iso3)}</strong>: ${matched.length} qualifying publication(s)
        </div>
        <div style="display:flex; gap:24px; flex-wrap:wrap; margin-bottom:8px">
          <div>
            <em>Cohort authors (that satisfy the FL/M rule)</em>
            ${authorList ? `<ul>${authorList}</ul>` : '<div class="muted">None detected in cohort (or missing IDs).</div>'}
          </div>
        </div>
        <div>
          <em>Publications</em>
          <ul>${pubList}</ul>
          ${matched.length > 30 ? '<div class="muted">…truncated</div>' : ''}
        </div>
      `;
    }

    function normalizeID(x){
      return String(x||'').trim()
        .replace(/^https?:\/\/openalex\.org\/authors\//i,'')
        .replace(/^https?:\/\/openalex\.org\//i,'')
        .trim();
    }

    function getYearBounds(){
      try {
        if (yearBounds && Number.isFinite(yearBounds.min) && Number.isFinite(yearBounds.max)) {
          return yearBounds;
        }
      } catch(_) {}
      const now = new Date().getFullYear();
      return { min: now - 4, max: now };
    }


    function _fiveYearSpan(){
      const { min: yMin, max: yMax } = getYearBounds();
      const end = Number.isFinite(yMax) ? yMax : new Date().getFullYear();
      const start = Math.max(end - 4, Number.isFinite(yMin) ? yMin : (end - 4));
      const years = [];
      for (let y = start; y <= end; y++) years.push(y);
      return years;
    }
    
    function computeFMLSeriesForAuthor(authorOpenAlexID){
      // --- helpers / fallbacks (kept local to avoid globals issues) ---
      const normID = (x) => String(x||'').trim()
        .replace(/^https?:\/\/openalex\.org\/authors\//i,'')
        .replace(/^https?:\/\/openalex\.org\//i,'')
        .trim();
    
      const years = _fiveYearSpan(); // exact years from the year inputs
      const byYear = new Map(years.map(y => [y, { year: y, first: 0, middle: 0, last: 0, total: 0 }]));
    
      const onTypes = (typeof gatherActiveTypes === 'function') ? gatherActiveTypes() : new Set();
      const topicQ  = (document.getElementById('topic-search')?.value || '').trim().toLowerCase();
    
      const fuzzyMatch = (q, hay) => {
        if (!q) return true;
        const hayL = String(hay || '').toLowerCase();
        if (typeof fuzzyQueryMatch === 'function') return fuzzyQueryMatch(q, hayL);
        // fallback: all whitespace tokens present
        const toks = q.split(/\s+/).filter(Boolean);
        return toks.every(t => hayL.includes(t));
      };
    
      const buildTopicHaystack = (r) => {
        if (r._topic_haystack) return r._topic_haystack; // normalized during load
        const parts = [
          r.display_name, r.title, r.abstract,
          r.concepts__display_name, r.topics__display_name,
          r.authorships__author__display_name
        ].map(x => String(x || ''));
        return parts.join(' | ').toLowerCase();
      };
    
      const normalizeType = (r) => {
        if (r._type_norm) return String(r._type_norm).toLowerCase();
        return String(r.type || '').toLowerCase() || 'other';
      };
    
      const aid = normID(authorOpenAlexID);
      if (!Array.isArray(perAuthorData) || !aid) {
        return years.map(y => ({ year: y, first: 0, middle: 0, last: 0, total: 0 }));
      }
    
      // Derive this author’s position on this work if ETL field missing
      const derivePositionIfMissing = (row) => {
        const has = String(row.this_author_position || '').trim();
        if (has) return has.toLowerCase();
    
        const ids = String(row.authorships__author__id || '')
          .split('|').map(s => normID(s));
        const poss = String(row.authorships__author_position || '')
          .split('|').map(s => String(s).toLowerCase().trim());
        const idx = ids.findIndex(x => x === aid);
        return (idx >= 0 && idx < poss.length) ? poss[idx] : '';
      };
    
      for (const r of perAuthorData) {
        if (normID(r.author_openalex_id) !== aid) continue;
    
        // Year window
        const y = parseInt(r.publication_year || r.year || r.from_year || '', 10);
        if (!byYear.has(y)) continue;
    
        // Type filter (legend)
        const t = normalizeType(r);
        if (onTypes.size && !onTypes.has(t)) continue;
    
        // Topic filter (search box)
        if (topicQ && !fuzzyMatch(topicQ, buildTopicHaystack(r))) continue;
    
        const pos = derivePositionIfMissing(r);
        const bucket = byYear.get(y);
        if (pos === 'first')      bucket.first++;
        else if (pos === 'last')  bucket.last++;
        else if (pos)             bucket.middle++;
        // Always count in total if the record passed all filters
        bucket.total++;
      }
    
      return years.map(y => byYear.get(y));
    }


    
    function renderMiniFMLSVG(series){
      if (!Array.isArray(series) || !series.length) return '<div class="mini-fml"></div>';
    
      // Normalize series: accept either numbers (totals) or objects
      const norm = series.map((s, i) => {
        if (s && typeof s === 'object') {
          const year   = Number.isFinite(s.year) ? s.year : i;
          const first  = Number(s.first  || 0);
          const middle = Number(s.middle || 0);
          const last   = Number(s.last   || 0);
          const total  = Number(s.total  || (first + middle + last) || 0);
          return { year, first, middle, last, total };
        } else {
          const total = Number(s || 0);
          return { year: i, first: 0, middle: 0, last: 0, total };
        }
      });
    
      const H = 22, W = 11, GAP = 3;
      const maxTot = Math.max(1, ...series.map(s => s.total || 0));
      const width = norm.length * (W + GAP) - GAP;
    
      const parts = [`<div class="mini-fml"><svg viewBox="0 0 ${width} ${H}" aria-label="Authorship mix by year">`];
    
      norm.forEach((s, i) => {
        const x = i * (W + GAP);
        let y0 = H;
    
        // Proportional heights; clamp to integers
        const hFirst  = Math.round(((s.first  || 0) / maxTot) * H);
        const hMiddle = Math.round(((s.middle || 0) / maxTot) * H);
        const hLast   = Math.round(((s.last   || 0) / maxTot) * H);
    
        // First (bottom)
        y0 -= hFirst;
        if (hFirst > 0) {
          parts.push(
            `<rect class="seg-first" x="${x}" y="${y0}" width="${W}" height="${hFirst}">` +
            `<title>${s.year}: first ${s.first}</title></rect>`
          );
        }
    
        // Middle (stack)
        y0 -= hMiddle;
        if (hMiddle > 0) {
          parts.push(
            `<rect class="seg-middle" x="${x}" y="${y0}" width="${W}" height="${hMiddle}">` +
            `<title>${s.year}: middle ${s.middle}</title></rect>`
          );
        }
    
        // Last (top)
        y0 -= hLast;
        if (hLast > 0) {
          parts.push(
            `<rect class="seg-last" x="${x}" y="${y0}" width="${W}" height="${hLast}">` +
            `<title>${s.year}: last ${s.last}</title></rect>`
          );
        }
      });
    
      parts.push(`</svg></div>`);
      return parts.join('');
    }



    
    function drawFacultyTable(faculty){
      const body = document.querySelector('#faculty-table tbody');
      body.innerHTML = '';
      const countEl = document.getElementById('selected-faculty-count');
      if (countEl) countEl.textContent = ` (n=${Array.isArray(faculty) ? faculty.length : 0})`;
    
      // Sort by H-index desc
      faculty.sort((a,b) => (toInt(b.H_index) - toInt(a.H_index)) || String(a.Name||'').localeCompare(String(b.Name||'')));
    
      faculty.forEach(f => {
        // Normalize OpenAlex author id for URL
        const openAlexIdRaw = String(f.OpenAlexID || '').trim();
        const openAlexId = openAlexIdRaw.replace(/^https?:\/\/openalex\.org\/authors\//i, '').trim();
        const openAlexURL = openAlexId ? `https://openalex.org/authors/${openAlexId}` : '';
    
        // ORCID (number only), tiny font, inline next to OpenAlex link
        const orcidRaw = String((f.ORCID ?? f.Orcid ?? '')).trim();
        const orcidId = orcidRaw.replace(/^https?:\/\/orcid\.org\//i, '').trim();
        const orcidHTML = orcidId
          ? `&nbsp;·&nbsp;<a href="https://orcid.org/${escapeHTML(orcidId)}" target="_blank" rel="noopener" class="meta-link meta-inline">${escapeHTML(orcidId)}</a>`
          : '';
    
        // Build the 5y F/M/L series + mini SVG + total
        const series = computeFMLSeriesForAuthor(f.OpenAlexID); // [{year,first,middle,last,total}, ...]
        const mini   = renderMiniFMLSVG(series);
        
        // Sum "total" across the 5 years
        let total5y = Array.isArray(series)
          ? series.reduce((acc, s) => acc + (s && typeof s === 'object' ? (s.total || 0) : 0), 0)
          : 0;
        
        // If we're focused on this author, you can opt to show the Publications panel's count
        // (which is already type/topic filtered and de-duplicated)
        if (focusedAuthorID &&
            normalizeID(f.OpenAlexID) === normalizeID(focusedAuthorID) &&
            Array.isArray(lastSelectedPubs)) {
          total5y = lastSelectedPubs.length;
        }

    
        const row = `<tr>
          <td>
            <div>
              <a href="#" class="author-link" data-id="${escapeHTML(f.OpenAlexID)}" data-name="${escapeHTML(f.Name)}">${escapeHTML(f.Name)}</a>
              ${openAlexURL ? `&nbsp;·&nbsp;<a href="${openAlexURL}" target="_blank" rel="noopener" class="meta-link meta-inline">OpenAlex profile</a>` : ''}
              ${orcidHTML}
            </div>
          </td>
          <td>${mini}</td>
          <td class="mono">${total5y}</td>
          <td>${toInt(f.H_index)}</td>
          <td>${toInt(f.I10_index)}</td>
          <td>${toInt(f.Works_count)}</td>
          <td>${toInt(f.Total_citations)}</td>
        </tr>`;
    
        body.insertAdjacentHTML('beforeend', row);
      });
    
      // Click to focus on one author (local link)
      body.querySelectorAll('.author-link').forEach(a => {
        a.addEventListener('click', (e) => {
          e.preventDefault();
          focusedAuthorID = a.getAttribute('data-id');
          focusedAuthorName = a.getAttribute('data-name');
          update();
        });
      });
    }




function drawPublicationList(pubs) {
  const ul = document.getElementById('publications-list');
  if (!ul) return;

  ul.innerHTML = '';

  // Guard: no data or empty after filtering
  if (!Array.isArray(pubs) || pubs.length === 0) {
    const li = document.createElement('li');
    li.className = 'muted';
    li.textContent = 'No publications match the current filters.';
    ul.appendChild(li);
    // Optional: update a visible counter if you have one
    const countEl = document.getElementById('publications-count');
    if (countEl) countEl.textContent = '0';
    return;
  }

  // Sort newest first, then by citations (desc)
  pubs.sort((a, b) =>
    (b.publication_year - a.publication_year) ||
    (b.cited_by_count - a.cited_by_count)
  );

  const frag = document.createDocumentFragment();

  pubs.forEach(p => {
    const year = toInt(p.publication_year);
    const safeTitle = allowItalicsOnly(p.display_name || '');
    const type = escapeHTML(p.type || '');

    // Normalize DOI field: expect full URL; otherwise omit
    const doi = (p.doi && /^https?:\/\//i.test(p.doi)) ? p.doi : '';

    // Normalize FWCI to a finite number if present on record (e.g., as string)
    let fwci = (p && p._fwci != null) ? Number(p._fwci) : NaN;
    fwci = Number.isFinite(fwci) ? fwci : NaN;

    const li = document.createElement('li');

    // Main line + meta chips
    li.innerHTML = `
      <div><strong>${year}</strong> — <em>${safeTitle}</em> <span class="muted">(${type})</span></div>
      <div class="pub-meta">
        <span class="chip"><span class="mono">Citations:</span> ${toInt(p.cited_by_count)}</span>
        ${Number.isFinite(fwci) ? `<span class="chip secondary"><span class="mono">FWCI:</span> ${fwci.toFixed(2)}</span>` : ''}
        ${doi ? `<a class="chip" href="${doi}" target="_blank" rel="noopener">DOI</a>` : ''}
      </div>
    `;

    frag.appendChild(li);
  });

  ul.appendChild(frag);

  // Optional: update a visible counter if your HTML has one
  const countEl = document.getElementById('publications-count');
  if (countEl) countEl.textContent = String(pubs.length);
}


    // ============ Text helpers ============
    function escapeHTML(s){
      return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }

    // Allow <i> and </i> only (for italic species names)
    function allowItalicsOnly(s){
      const escaped = escapeHTML(String(s||''));
      // Restore *only* <i> and </i> tags if they existed literally
      // (assuming input titles may include <i>...</i>)
      return escaped
        .replace(/&lt;i&gt;/g, '<i>')
        .replace(/&lt;\/i&gt;/g, '</i>');
    }

  function stripDiacritics(s){ return String(s||'').normalize('NFKD').replace(/[\u0300-\u036f]/g,''); }

function canonName(raw){
  let s = stripDiacritics(raw).toLowerCase().trim();
  const m = s.match(/^([^,]+),\s*(.+)$/); if (m) s = `${m[2]} ${m[1]}`;
  s = s.replace(/[.'’-]/g, ' ').replace(/[^a-z\s]/g,' ').replace(/\s+/g,' ').trim();
  const parts = s.split(' ').filter(Boolean);
  if (parts.length === 1) return parts[0];
  const suff = new Set(['jr','sr','iii','ii']); while (parts.length>1 && suff.has(parts.at(-1))) parts.pop();
  const particles = new Set(['de','del','della','der','den','van','von','da','di','dos','la','le','mac','mc','bin','al','ibn','st','st.']);
  const first = parts[0]; let last = parts.at(-1); const pen = parts.at(-2);
  if (parts.length>=3 && particles.has(pen)) last = `${pen} ${last}`;
  return `${first[0]} ${last}`;
}

function splitAuthorsList(s){ return s ? String(s).split(/\s*;\s*/).map(t=>t.trim()).filter(Boolean) : []; }

// --- Author/roster metadata helper (for PCA coloring) ---
function authorMetaFromRoster(authorId, roster){
  const norm = (s) => String(s||'')
    .replace(/^https?:\/\/openalex\.org\/authors\//i, '')
    .trim();
  const row = (roster || []).find(r => norm(r.OpenAlexID) === authorId);
  if(!row) return { groups: [], isFull: false };

  const rgKeys = Object.keys(row).filter(k => /^RG\d+$/i.test(k));
  const groups = [];
  for(const k of rgKeys){
    const v = String(row[k] ?? '').trim();
    if(!v) continue;
    if (/^(y(es)?|true|1|member|full)$/i.test(v)) groups.push(k.toUpperCase());
    else groups.push(v);
  }

  const isFull =
    rgKeys.some(k => /full/i.test(String(row[k]||''))) ||
    /full/i.test(String(row.Membership || row.MemberType || row.Status || ''));

  return { groups: Array.from(new Set(groups)), isFull };
}

// ============ Fuzzy search helpers (STRICT) ============
// Goal: high precision. No prefix or edit-distance fuzziness.
// Match = exact token equality after conservative stemming.

function normalizeText(t) {
  if (t == null) return "";
  // Unicode normalize, strip diacritics, lowercase, collapse punctuation/whitespace
  let s = String(t)
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")  // remove combining marks
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")      // map non-alphanumerics to spaces
    .replace(/\s+/g, " ")             // collapse spaces
    .trim();
  return s;
}

// Conservative stemmer: plurals and common verb endings only.
// Intentionally avoids aggressive derivational stemming to preserve meaning.
function stem(w) {
  if (!w || w.length <= 3) return w;  // don't stem very short tokens

  let s = w;

  // plural -> singular
  if (s.endsWith("sses")) {
    s = s.slice(0, -2);                                // "classes" -> "class"
  } else if (s.endsWith("ies") && s.length > 4) {
    s = s.slice(0, -3) + "y";                          // "studies" -> "study"
  } else if (s.endsWith("s") && !s.endsWith("ss") && s.length > 3) {
    s = s.slice(0, -1);                                // "dogs" -> "dog"
  }

  // past/gerund
  if (s.endsWith("ing") && s.length > 5) {
    s = s.slice(0, -3);                                // "running" -> "runn"
    if (s.length > 3 && s[s.length - 1] === s[s.length - 2]) {
      s = s.slice(0, -1);                              // "runn" -> "run"
    }
  } else if (s.endsWith("ed") && s.length > 4) {
    s = s.slice(0, -2);                                // "jogged" -> "jogg"
    if (s.length > 3 && s[s.length - 1] === s[s.length - 2]) {
      s = s.slice(0, -1);                              // "jogg" -> "jog"
    }
  }

  return s;
}

function tokenize(text) {
  // Normalize -> split -> light stem -> deduplicate
  const base = normalizeText(text).split(/\s+/).filter(Boolean).map(stem);
  return Array.from(new Set(base));
}

// STRICT token match: exact equality only after stemming
function strongTokenMatch(qt, tt) {
  return tt === qt;
}

// AND semantics across query tokens; every query token must match some text token.
function fuzzyQueryMatch(query, text) {
  const qTokens = tokenize(query);
  if (!qTokens.length) return true;

  const tTokens = tokenize(text);

  // Precision guardrails:
  // - 1–2 char tokens: must match exactly (already enforced by equality)
  // - 3+ char tokens: equality after stemming only; no prefixes, no edit distance
  return qTokens.every(qt => tTokens.some(tt => strongTokenMatch(qt, tt)));
}

// ========== End Fuzzy search helpers ==========

function exportCurrentSelectionCSV(pubs) {
  // Desired column order and their source fields in your CSV rows
  // Your CSV already includes these columns (confirmed): 
  // publication_year, display_name, authors, host_venue__display_name, id, doi, fwci, cited_by_count, type, institutions, concepts_list
  const headers = [
    'publication_year',
    'title',
    'authors',
    'journal',
    'id',
    'DOI',
    'FWCI',
    'citations',
    'type',
    'institutions',
    'concepts_list'
  ];

  // map a pub row to the exact ordered values
  const rows = pubs.map(p => ([
    safeCsv(p.publication_year),
    safeCsv(p.display_name),                    // title
    safeCsv(p.authors),
    safeCsv(p.host_venue__display_name),        // journal
    safeCsv(p.id),
    safeCsv(p.doi),
    safeCsv((Number.isFinite(p._fwci) ? p._fwci : p.fwci)), // FWCI (use parsed _fwci if present)
    safeCsv(p.cited_by_count),                  // citations
    safeCsv(p.type),
    safeCsv(p.institutions),
    safeCsv(p.concepts_list)
  ]));

  // Build CSV (RFC4180-ish): quote fields, double internal quotes
  const headerLine = headers.map(csvEscape).join(',');
  const bodyLines = rows.map(r => r.map(csvEscape).join(',')).join('\n');
  const csv = headerLine + '\n' + bodyLines + '\n';

  // Download (with UTF-8 BOM so Excel opens cleanly)
  const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'publications_selection.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function csvEscape(v) {
  const s = String(v == null ? '' : v);
  // Escape CR/LF to keep rows intact; many CSV readers handle raw newlines but this is safer
  const cleaned = s.replace(/\r\n/g, ' ').replace(/\n/g, ' ').replace(/\r/g, ' ');
  // If field contains comma, quote, or leading/trailing space, wrap in quotes and double quotes inside
  if (/[",\s]/.test(cleaned[0] || '') || /[",\s]/.test(cleaned.slice(-1)) || /[",\n]/.test(cleaned)) {
    return `"${cleaned.replace(/"/g, '""')}"`;
  }
  return cleaned;
}

function safeCsv(v) {
  if (v == null) return '';
  // prefer numbers as-is; everything else to string
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  return String(v);
}

function setExportButtonCount(n) {
  const btn = document.getElementById('export-selection');
  if (!btn) return;
  btn.textContent = `Export current selection (n=${n})`;
  btn.disabled = (n === 0);                   // optional UX: disable when empty
  btn.classList.toggle('is-disabled', n === 0);
}

// ==================== Co-authorship Network & Pairs Table ====================


// Main updater
function updateCoauthorPanels(contributingRoster, selectedPubs){
  const graph = computeCoauthorGraph(contributingRoster, selectedPubs);
  drawCoauthorNetwork(graph);
  drawCoauthorPairsTable(graph);
  const meta = document.getElementById('network-meta');
  if (meta) {
    meta.textContent = `${graph.nodes.length} researchers · ${graph.edges.length} links`;
  }
}

function computeCoauthorGraph(contributingRoster, selectedPubs){
  const idNorm = s => String(s||'').trim()
    .replace(/^https?:\/\/openalex\.org\/authors\//i,'')
    .replace(/^https?:\/\/openalex\.org\//i,'');
  const workNorm = s => String(s||'').trim()
    .replace(/^https?:\/\/openalex\.org\/works\//i,'')
    .replace(/^https?:\/\/openalex\.org\//i,'');

  // roster id -> display name
  const nameOf = new Map(contributingRoster.map(r => [idNorm(r.OpenAlexID), r.Name || r.OpenAlexID]));

  const levelOf = new Map(contributingRoster.map(r => [
  idNorm(r.OpenAlexID),
  (r.Level || '').trim()
  ]));
  
  // Respect all active filters by limiting to the works in the current (dedup) selection
  const selectedWorkIDs = new Set(
    selectedPubs.map(p => workNorm(p.id || p.work_id || '')).filter(Boolean)
  );

  // === Preferred path: expand authorships from PRE-dedup ===
  if (typeof authorshipData !== 'undefined' && Array.isArray(authorshipData) && authorshipData.length) {
    // Build work -> Set(roster_author_ids) for the selected works only
    const byWork = new Map();
    for (const row of authorshipData) {
      const wid = workNorm(row.id || row.work_id || '');
      if (!selectedWorkIDs.has(wid)) continue;     // only current selection
      const aid = idNorm(row.author_openalex_id);
      if (!nameOf.has(aid)) continue;              // only cohort authors
      if (!byWork.has(wid)) byWork.set(wid, new Set());
      byWork.get(wid).add(aid);
    }
    return buildGraphFromPairs(byWork, nameOf, selectedPubs);
  }

  // === Fallback: derive from semicolon-joined author strings in the dedup rows ===
  const rosterByCanon = new Map(); // canon -> [{id, name}]
  for (const r of contributingRoster) {
    const cid = idNorm(r.OpenAlexID);
    const cname = canonName(r.Name || '');
    if (!rosterByCanon.has(cname)) rosterByCanon.set(cname, []);
    rosterByCanon.get(cname).push({ id: cid, name: r.Name || cid });
  }

  const byWork = new Map();
  for (const p of selectedPubs) {
    const wid = workNorm(p.id || p.work_id || '');
    if (!wid) continue;
    const names = splitAuthorsList(p.authors);
    const idsOnWork = new Set();
    for (const n of names) {
      const hits = rosterByCanon.get(canonName(n));
      if (hits) for (const h of hits) idsOnWork.add(h.id);
    }
    if (idsOnWork.size >= 2) byWork.set(wid, idsOnWork);
  }

  return buildGraphFromPairs(byWork, nameOf, selectedPubs);

  // ---- helper: convert work->authors into nodes/edges, deduping by work id
  function buildGraphFromPairs(byWork, nameOf, selectedPubs){
    // representative pub row per work (for edge click lists)
    const widToPub = new Map();
    const widOf = p => workNorm(p?.id || p?.work_id || '');
    for (const p of selectedPubs) { const w = widOf(p); if (w && !widToPub.has(w)) widToPub.set(w, p); }

    const pairCounts = new Map(); // "a|b" -> count
    const pairPubs   = new Map(); // "a|b" -> [pubRows]

    for (const [wid, set] of byWork.entries()){
      const ids = Array.from(set).sort();
      for (let i=0;i<ids.length;i++){
        for (let j=i+1;j<ids.length;j++){
          const a=ids[i], b=ids[j], key=`${a}|${b}`;
          pairCounts.set(key, (pairCounts.get(key)||0) + 1);
          if (!pairPubs.has(key)) pairPubs.set(key, []);
          pairPubs.get(key).push(widToPub.get(wid)); // safe: may hold a row from selectedPubs
        }
      }
    }

    // nodes
    const nodeIDs = new Set();
    for (const key of pairCounts.keys()){ const [a,b]=key.split('|'); nodeIDs.add(a); nodeIDs.add(b); }
    const nodes = Array.from(nodeIDs).map(id => ({ id, name: nameOf.get(id), level: levelOf.get(id) || id }));
    
      
    // edges (dedup the pub list by work id)
    const idxOf = new Map(nodes.map((n,i)=>[n.id,i]));
    const edges = [];
    for (const [key, count] of pairCounts.entries()){
      const [a,b] = key.split('|');
      if (!idxOf.has(a) || !idxOf.has(b)) continue;
      const seen = new Set(); const uniqPubs = [];
      for (const p of (pairPubs.get(key) || [])) {
        const wid = widOf(p); if (wid && !seen.has(wid)) { seen.add(wid); uniqPubs.push(p); }
      }
      edges.push({ a, b, ai: idxOf.get(a), bi: idxOf.get(b), count, pubs: uniqPubs });
    }

    // simple circular layout + degree for size
    const N = nodes.length, R = 0.85;
    nodes.forEach((n,i)=>{ const t=(i/Math.max(1,N))*2*Math.PI; n.x=R*Math.cos(t); n.y=R*Math.sin(t); });
    const degree = new Map(nodes.map(n => [n.id, 0]));
    edges.forEach(e => { degree.set(e.a,(degree.get(e.a)||0)+e.count); degree.set(e.b,(degree.get(e.b)||0)+e.count); });
    nodes.forEach(n => n.deg = degree.get(n.id) || 0);

    return { nodes, edges };
  }
}


function drawCoauthorNetwork(graph){
  const el = document.getElementById('coauthor-network');
  if (!el) return;
  if (!graph || !Array.isArray(graph.nodes) || graph.nodes.length === 0) {
    if (window.Plotly && el) Plotly.purge(el);
    return;
  }

  // Node positions, labels, sizes (degree-scaled)
  const xs = graph.nodes.map(n => n.x);
  const ys = graph.nodes.map(n => n.y);
  const labels = graph.nodes.map(n => n.name);
  const degs = graph.nodes.map(n => n.deg ?? 0);
  const minDeg = Math.min(...degs, 0);
  const maxDeg = Math.max(...degs, 1);
  const size = degs.map(d => {
    const t = (d - minDeg) / (maxDeg - minDeg || 1);
    return 10 + t * 18; // 10..28 px
  });

  // Build edge line traces + invisible midpoint click targets
  const edgeLineTraces = [];
  const edgeClickTargetsX = [];
  const edgeClickTargetsY = [];
  const edgeClickTargetsCustom = [];

  let minW = Infinity, maxW = 0;
  graph.edges.forEach(e => { minW = Math.min(minW, e.count); maxW = Math.max(maxW, e.count); });
  const lineWidth = (c) => {
    if (!Number.isFinite(c)) return 1;
    if (minW === maxW) return 4; // constant width when all equal
    const t = (c - minW) / (maxW - minW);
    return 1 + t * 8; // 1..9 px
  };

  graph.edges.forEach(e => {
    const x0 = graph.nodes[e.ai].x, y0 = graph.nodes[e.ai].y;
    const x1 = graph.nodes[e.bi].x, y1 = graph.nodes[e.bi].y;

    edgeLineTraces.push({
      type: 'scatter',
      mode: 'lines',
      x: [x0, x1],
      y: [y0, y1],
      hoverinfo: 'skip',
      line: { width: lineWidth(e.count), color: 'rgba(100,116,139,0.6)' },
      showlegend: false
    });

    // Midpoint markers used for clicking an edge to show joint pubs
    const mx = (x0 + x1) / 2, my = (y0 + y1) / 2;
    edgeClickTargetsX.push(mx);
    edgeClickTargetsY.push(my);
    edgeClickTargetsCustom.push(`${e.a}|${e.b}`);
  });

  const edgeClickTrace = {
    type: 'scatter',
    mode: 'markers',
    x: edgeClickTargetsX,
    y: edgeClickTargetsY,
    customdata: edgeClickTargetsCustom,
    marker: { size: 12, opacity: 0.005 },
    name: 'edge-click-targets',
    hoverinfo: 'skip',
    showlegend: false
  };

  // Robust node hovertext: collaborator list with counts
  const idToName = new Map(graph.nodes.map(n => [n.id, n.name]));
  const hoverText = graph.nodes.map(n => {
    const partners = graph.edges
      .filter(e => e.a === n.id || e.b === n.id)
      .map(e => {
        const partnerId = (e.a === n.id) ? e.b : e.a;
        const partnerName = idToName.get(partnerId) || partnerId;
        return { name: partnerName, count: e.count };
      })
      .sort((A, B) => (B.count - A.count) || A.name.localeCompare(B.name))
      .map(p => `• ${escapeHTML(p.name)} (${p.count})`);
    return `<b>${escapeHTML(n.name)}</b><br>${partners.join('<br>') || 'No in-cohort co-authors in selection'}`;
  });

  // Build per-node outline style from Level
  const outlineForLevel = (lvl) => {
    const L = String(lvl || '').toLowerCase();
    if (L.startsWith('assistant')) return { color: '#3b82f6', width: 2 };
    if (L.startsWith('associate')) return { color: '#f59e0b', width: 3 };
    if (L.startsWith('prof'))      return { color: '#10b981', width: 4 };
    return { color: '#94a3b8', width: 1 }; // fallback: slate-300
  };
  const outlineColors = graph.nodes.map(n => outlineForLevel(n.level).color);
  const outlineWidths = graph.nodes.map(n => outlineForLevel(n.level).width);

  // Nodes
  const nodeTrace = {
    type: 'scatter',
    mode: 'markers+text',
    x: xs,
    y: ys,
    text: labels,
    textposition: 'top center',
    hovertext: hoverText,
    hovertemplate: '%{hovertext}<extra></extra>',
    cliponaxis: false, // allow labels to slightly extend past axes
    marker: {
      size: size,
      color: '#ffffff', // keep interior white for contrast
      line: { width: outlineWidths, color: outlineColors } // per-node arrays
    },
    name: 'Authors',
    showlegend: false
  };

  // Pair index for click handler (edge midpoint -> pubs)
  const pairIndex = {};
  graph.edges.forEach(e => { pairIndex[`${e.a}|${e.b}`] = e; });
  el.__pairIndex = pairIndex;

  // Legend-only items to explain outline = Level
  const legendAssistant = {
    type: 'scatter', mode: 'markers',
    x: [null], y: [null],
    name: 'Assistant',
    marker: { size: 14, color: 'rgba(255,255,255,1)', line: { color: '#3b82f6', width: 2 } },
    showlegend: true,
    hoverinfo: 'skip'
  };
  const legendAssociate = {
    type: 'scatter', mode: 'markers',
    x: [null], y: [null],
    name: 'Associate',
    marker: { size: 14, color: 'rgba(255,255,255,1)', line: { color: '#f59e0b', width: 3 } },
    showlegend: true,
    hoverinfo: 'skip'
  };
  const legendProfessor = {
    type: 'scatter', mode: 'markers',
    x: [null], y: [null],
    name: 'Professor',
    marker: { size: 14, color: 'rgba(255,255,255,1)', line: { color: '#10b981', width: 4 } },
    showlegend: true,
    hoverinfo: 'skip'
  };

  // SINGLE layout declaration (removed the earlier duplicate)
  const layout = {
    xaxis: { visible: false, range: [-1.1, 1.1] },
    yaxis: { visible: false, range: [-1.1, 1.1] },
    margin: { t: 10, r: 40, b: 10, l: 40 }, // more left/right room for labels
    height: 360,
    hovermode: 'closest',
    plot_bgcolor: 'rgba(0,0,0,0)',
    paper_bgcolor: 'rgba(0,0,0,0)',
    legend: { title: { text: 'Level (node outline)' } }
  };

  




  
  // Render
  Plotly.react(
    el,
    [legendAssistant, legendAssociate, legendProfessor, ...edgeLineTraces, edgeClickTrace, nodeTrace],
    layout,
    { displayModeBar: false }
  );

  // Attach click handler once (after Plotly has initialized)
  if (!el.__clickBound && typeof el.on === 'function') {
    el.on('plotly_click', (ev) => {
      const pt = ev?.points?.[0];
      if (!pt) return;
      const isEdgeClickTargets = pt?.data?.name === 'edge-click-targets';
      const pairKey = pt?.customdata;
      if (isEdgeClickTargets && pairKey) {
        const ctx = el.__pairIndex || {};
        const rec = ctx[pairKey];
        if (rec) showPairPublications(rec.a, rec.b, rec.pubs);
      }
    });
    el.__clickBound = true;
  }
}



function drawCoauthorPairsTable(graph){
  const body = document.querySelector('#coauthor-table tbody');
  if (!body) return;
  body.innerHTML = '';

  // Sort pairs by count desc, then alpha
  const rows = graph.edges
    .map(e => ({ a: graph.nodes[e.ai].name, b: graph.nodes[e.bi].name, key: `${e.a}|${e.b}`, count: e.count, pubs: e.pubs }))
    .sort((r1, r2) => (r2.count - r1.count) || (r1.a.localeCompare(r2.a)) || (r1.b.localeCompare(r2.b)));

  const frag = document.createDocumentFragment();
  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${escapeHTML(r.a)}</td><td>${escapeHTML(r.b)}</td><td>${r.count}</td>`;
    tr.addEventListener('click', () => {
      showPairPublications(r.key.split('|')[0], r.key.split('|')[1], r.pubs);
      // Optional: scroll into view
      document.getElementById('pair-detail')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    });
    frag.appendChild(tr);
  });
  body.appendChild(frag);
}

function showPairPublications(aID, bID, pubs){
  const box = document.getElementById('pair-detail');
  if (!box) return;

  // Name lookup via rosterData
  const norm = (s)=> String(s||'').replace(/^https?:\/\/openalex\.org\/authors\//i,'').replace(/^https?:\/\/openalex\.org\//i,'');
  const nameOf = new Map(rosterData.map(r => [norm(r.OpenAlexID), r.Name || r.OpenAlexID]));
  const nameA = nameOf.get(norm(aID)) || aID;
  const nameB = nameOf.get(norm(bID)) || bID;

  if (!pubs || !pubs.length) {
    box.innerHTML = `<div class="muted">No publications found for ${escapeHTML(nameA)} and ${escapeHTML(nameB)} in the current selection.</div>`;
    return;
  }

  // Dedup by work id again (safety)
  const widOf = (p) => String(p?.id || p?.work_id || '').replace(/^https?:\/\/openalex\.org\/works\//i,'').replace(/^https?:\/\/openalex\.org\//i,'');
  const seen = new Set();
  const list = [];
  pubs.forEach(p => {
    const wid = widOf(p);
    if (!wid || seen.has(wid)) return;
    seen.add(wid);
    list.push(p);
  });

  const items = list.map(p => {
    const year = toInt(p.publication_year);
    const title = allowItalicsOnly(p.display_name || '');
    const doi = (p.doi && /^https?:\/\//i.test(p.doi)) ? `<a href="${p.doi}" target="_blank" rel="noopener">DOI</a>` : '';
    const idLink = p.id ? `<a href="${p.id}" target="_blank" rel="noopener">OpenAlex</a>` : '';
    const journal = escapeHTML(p.host_venue__display_name || '');
    return `<li><strong>${year}</strong> — <em>${title}</em> <span class="muted">(${journal})</span> ${doi ? '· '+doi : ''} ${idLink ? '· '+idLink : ''}</li>`;
  }).join('');

  box.innerHTML = `
    <div class="chip">Papers co-authored by <strong>${escapeHTML(nameA)}</strong> and <strong>${escapeHTML(nameB)}</strong> (n=${list.length})</div>
    <ul class="pair-pubs">${items}</ul>
  `;
}

    
    // ============ Utilities ============
    function uniqueNonEmpty(arr){
      return Array.from(new Set(arr.filter(v => v && String(v).trim() !== ''))).sort();
    }

    function debounce(fn, ms){
      let t=null;
      return (...args) => {
        clearTimeout(t);
        t = setTimeout(() => fn.apply(null, args), ms);
      };
    }
  });
})();
