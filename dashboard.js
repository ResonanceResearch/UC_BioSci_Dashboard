// dashboard.js — full version with OpenAlex profile links and FWCI in publications
(function(){
  // ==== Defaults ====
  const DEFAULT_START_YEAR = 2021;
  const DEFAULT_END_YEAR = 2025;

  document.addEventListener('DOMContentLoaded', () => {
    // Paths used by index.html
    const rosterPath = 'data/roster_with_metrics.csv';
    const pubsPath = 'data/openalex_all_authors_last5y_key_fields_dedup.csv';

    // In-memory data
    let rosterData = [];   // faculty roster + metrics
    let pubData = [];      // publications (last 5y)
    let yearBounds = { min: DEFAULT_START_YEAR, max: DEFAULT_END_YEAR };

    // Focus (single author) state
    let focusedAuthorID = null;
    let focusedAuthorName = '';

    // Load both CSVs, then initialize
    Promise.all([fetchCSV(rosterPath), fetchCSV(pubsPath)])
      .then(([rosterCSV, pubsCSV]) => {
        rosterData = parseCSV(rosterCSV);
        pubData = parseCSV(pubsCSV);

        normalizeRoster();
        normalizePubs();

        // UI bootstrap
        initFilters();
        initYearInputs();
        bindEvents();

        // Initial render
        update();
      })
      .catch(err => console.error('Failed to load CSVs', err));

    // ============ Core helpers ============
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

    function normalizePubs(){
      pubData.forEach(p => {
        // numeric
        p.publication_year = clampYear(p.publication_year);
        p.cited_by_count = toInt(p.cited_by_count);

        // author ID (already provided as 'author_openalex_id' per your CSV)
        p.author_openalex_id = normalizeID(p.author_openalex_id);

        // DOI: prefer full URL if present; otherwise leave empty
        if (p.doi && !/^https?:\/\//i.test(p.doi)) {
          // Some CSVs store DOIs like "10.1234/abcd"; we can link via dx.doi.org
          p.doi = 'https://doi.org/' + p.doi;
        }

        // FWCI (accept 'fwci' case from your CSV)
        p._fwci = toFloat(p.fwci);

        // Pre-compute a lowercase haystack for topic search:
        // concepts_list + subfield + primary topic + title (display_name)
        const c = [
          p.concepts_list || '',
          p.primary_topic__subfield__display_name || '',
          p.primary_topic__display_name || '',
          p.display_name || ''
        ].join(' ').toLowerCase();
        p._topic_haystack = normalizeText(c);
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

    function bindEvents(){
      // Multi-select changes
      document.querySelectorAll('#filters select').forEach(sel => sel.addEventListener('change', update));

      // Per-select Clear buttons
      document.querySelectorAll('.clear-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          const id = btn.getAttribute('data-target');
          const el = document.getElementById(id);
          Array.from(el.options).forEach(o => { o.selected = false; });
          // Restore default only for Appointment when its Clear is used
          if (id === 'appointment') setDefaultAppointmentSelection();
          update();
        });
      });

      // Year inputs
      document.getElementById('year-min').addEventListener('input', update);
      document.getElementById('year-max').addEventListener('input', update);

      // Topic search (concepts + subfields + primary topic + title)
      const topic = document.getElementById('topic-search');
      if (topic) topic.addEventListener('input', debounce(update, 200));

      // Global reset
      document.getElementById('reset-filters').addEventListener('click', () => {
        document.querySelectorAll('#filters select').forEach(sel => {
          Array.from(sel.options).forEach(o => o.selected = false);
        });
        // Restore default: only Full‑time selected
        setDefaultAppointmentSelection();

        focusedAuthorID = null;
        focusedAuthorName = '';
        initYearInputs();
        if (topic) topic.value = '';
        update();
      });
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
      const selectedCats = getMulti('category');
      const selectedAppt = getMulti('appointment');
      const selectedRGs = getMulti('research-group');

      // Filter pubs by year
      let pubs = pubData.filter(p => p.publication_year >= yearBounds.min && p.publication_year <= yearBounds.max);

      // Topic filter (robust token logic)
      if (topicQ) {
        pubs = pubs.filter(p => fuzzyQueryMatch(topicQ, p._topic_haystack));
      }

      let contributingRoster;
      if (focusedAuthorID) {
        // Focus: only pubs of that author
        const fa = normalizeID(focusedAuthorID);
        pubs = pubs.filter(p => normalizeID(p.author_openalex_id) === fa);
        // Roster: just that one entry, if present
        contributingRoster = rosterData.filter(r => normalizeID(r.OpenAlexID) === fa);
      } else {
        // Apply roster attribute filters to roster first
        const filteredRoster = rosterData.filter(r => {
          if (selectedLevels.length && !selectedLevels.includes(r.Level || '')) return false;
          if (selectedCats.length && !selectedCats.includes(r.Category || '')) return false;
          if (selectedAppt.length && !selectedAppt.includes(r.Appointment || '')) return false;
          if (selectedRGs.length && !r._RGs.some(g => selectedRGs.includes(g))) return false;
          return true;
        });

        // Then limit pubs to those whose author is in filteredRoster
        const allowedIDs = new Set(filteredRoster.map(r => normalizeID(r.OpenAlexID)));
        pubs = pubs.filter(p => allowedIDs.has(normalizeID(p.author_openalex_id)));

        // Contributing roster = filtered roster members who actually have pubs after all filters
        const havePubIDs = new Set(pubs.map(p => normalizeID(p.author_openalex_id)));
        contributingRoster = filteredRoster.filter(r => havePubIDs.has(normalizeID(r.OpenAlexID)));
      }

      return { contributingRoster, selectedPubs: pubs };
    }

    function getMulti(id){
      const el = document.getElementById(id);
      return Array.from(el.selectedOptions).map(o => o.value);
    }

    // ============ Rendering ============
    function update(){
      const { contributingRoster, selectedPubs } = applyFilters();
      drawBarChart(selectedPubs);
      drawFacultyTable(contributingRoster);
      drawPublicationList(selectedPubs);
      const fc = document.getElementById('faculty-count');
      const base = `Faculty contributing: ${contributingRoster.length}`;
      fc.textContent = focusedAuthorID ? `${base} (Focused: ${focusedAuthorName}. Use Reset to clear)` : base;
    }

    function drawBarChart(pubs){
      // Count by year x type
      const counts = new Map(); // key: `${year}::${type}` -> count
      const years = new Set();
      const types = new Set();

      pubs.forEach(p => {
        const y = p.publication_year;
        const t = (p.type || 'other').toLowerCase();
        years.add(y); types.add(t);
        const k = `${y}::${t}`;
        counts.set(k, (counts.get(k) || 0) + 1);
      });

      const sortedYears = Array.from(years).sort((a,b)=>a-b);
      const sortedTypes = Array.from(types).sort();

      // Build Plotly series (stacked bars)
      const traces = sortedTypes.map(t => {
        const yvals = sortedYears.map(y => counts.get(`${y}::${t}`) || 0);
        return {
          x: sortedYears,
          y: yvals,
          name: t,
          type: 'bar'
        };
      });

      const layout = {
        barmode: 'stack',
        xaxis: { title: 'Year', dtick: 1, range: [yearBounds.min - 0.5, yearBounds.max + 0.5] },
        yaxis: { title: 'Publications' },
        margin: { t: 20, r: 10, b: 40, l: 50 },
        height: 300
      };

      Plotly.react('pub-chart', traces, layout, {displayModeBar:false});
    }

    function drawFacultyTable(faculty){
      const body = document.querySelector('#faculty-table tbody');
      body.innerHTML = '';
      // Sort by H-index desc
      faculty.sort((a,b) => b.H_index - a.H_index);
      faculty.forEach(f => {
        const openAlexId = String(f.OpenAlexID || '').toLowerCase();
        const openAlexURL = `https://openalex.org/authors/${openAlexId}`;
        const row = `<tr>
          <td>
            <a href="#" class="author-link" data-id="${escapeHTML(f.OpenAlexID)}" data-name="${escapeHTML(f.Name)}">${escapeHTML(f.Name)}</a>
            &nbsp;·&nbsp;
            <a href="${openAlexURL}" target="_blank" rel="noopener">OpenAlex profile</a>
          </td>
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
