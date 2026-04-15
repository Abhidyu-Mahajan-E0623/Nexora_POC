var app = {
    TABLE_DESCRIPTIONS: {
        "hco_details": "Monitors healthcare organization data for unexpected changes in clinic tracking.",
        "hcp_details": "Monitors healthcare professional demographics and segment statuses.",
        "promo_activity": "Monitors promotional activities, tracking total calls and samples given out to professionals.",
        "sales_data": "Monitors local sales trends, anomalies and patterns per organization."
    },

    REVIEW_URL: "https://httpbin.org/html",

    /* Accepted state is loaded from the server's single threshold file */

    state: {
        currentView: "home",
        anomalyData: null,
        acceptedTables: {},
        acceptedSchemas: {},
        resolvedDetectors: {},
        abortController: null,
        filterCategory: "all",
        filterTable: "all",
        filterPeriod: "all"
    },

    navigate: function(viewId) {
        // If navigating away from loading, abort any pending pipeline
        if (this.state.currentView === 'loading' && viewId !== 'loading' && this.state.abortController) {
            this.state.abortController.abort();
            this.state.abortController = null;
        }

        var views = document.querySelectorAll(".view");
        for (var i = 0; i < views.length; i++) {
            views[i].classList.remove("visible");
            views[i].style.display = "none";
        }
        var target = document.getElementById("view-" + viewId);
        if (!target) return;
        target.style.display = "block";
        void target.offsetWidth;
        target.classList.add("visible");
        window.scrollTo({ top: 0, behavior: "smooth" });

        this.state.currentView = viewId;

        // Show sidebar and header nav for all inner pages (non-home)
        var isInner = (viewId !== "home");
        var sidebar = document.getElementById("app-sidebar");
        var mainArea = document.getElementById("main-area");
        var headerNav = document.getElementById("header-nav");

        if (sidebar) sidebar.style.display = isInner ? "flex" : "none";
        if (mainArea) {
            if (isInner) { mainArea.classList.add("with-sidebar"); }
            else { mainArea.classList.remove("with-sidebar"); }
        }
        if (headerNav) headerNav.style.display = isInner ? "flex" : "none";
    },

    goHome: function() {
        if (this.state.abortController) {
            this.state.abortController.abort();
            this.state.abortController = null;
        }
        this.navigate("home");
    },

    comingSoon: function(featureName) {
        this.showToast((featureName || "This feature") + " is coming soon!");
    },

    logSteps: [
        "Loading configuration and detector rules",
        "Connecting to Databricks SQL warehouse",
        "Loading monitored tables and schema snapshots",
        "Computing rolling data anomalies",
        "Inspecting rescue column payloads",
        "Classifying data and schema findings",
        "Saving report output"
    ],

    loadAnomaly: function(extraBody) {
        var self = this;
        self.navigate("loading");
        document.getElementById("loading-title").innerText = "Running Anomaly Monitor...";
        document.getElementById("loading-subtitle").innerText = "Connecting to Databricks cluster.";

        var logContainer = document.getElementById("step-log");
        logContainer.innerHTML = "";
        var stepEls = [];
        for (var i = 0; i < self.logSteps.length; i++) {
            var div = document.createElement("div");
            div.className = "step-item";
            div.innerHTML = '<div class="step-dot"></div><span>' + self.logSteps[i] + "</span>";
            logContainer.appendChild(div);
            stepEls.push(div);
        }

        var stepIdx = 0;
        var timer = setInterval(function() {
            if (stepIdx > 0 && stepIdx - 1 < stepEls.length) {
                stepEls[stepIdx - 1].classList.remove("active");
                stepEls[stepIdx - 1].classList.add("done");
            }
            if (stepIdx < stepEls.length) {
                stepEls[stepIdx].classList.add("active");
                document.getElementById("loading-subtitle").innerText = self.logSteps[stepIdx] + "...";
                stepIdx += 1;
            }
        }, 1400);

        var controller = new AbortController();
        self.state.abortController = controller;

        // Fire the pipeline (returns immediately)
        fetch("/api/anomaly", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(extraBody || { schema: "bronze" }),
            signal: controller.signal
        })
        .then(function(res) {
            if (!res.ok) throw new Error("Pipeline returned HTTP " + res.status);
            return res.json();
        })
        .then(function() {
            // Poll /api/anomaly/status until done
            var pollInterval = setInterval(function() {
                if (controller.signal.aborted) {
                    clearInterval(pollInterval);
                    return;
                }
                fetch("/api/anomaly/status", { signal: controller.signal })
                .then(function(res) { return res.json(); })
                .then(function(data) {
                    if (data.status === "done") {
                        clearInterval(pollInterval);
                        clearInterval(timer);
                        for (var j = 0; j < stepEls.length; j++) {
                            stepEls[j].classList.remove("active");
                            stepEls[j].classList.add("done");
                        }
                        self.state.abortController = null;
                        self.state.resolvedDetectors = {};
                        self.state.anomalyData = data.report_data || self.parsers.anomaly(data.report_text || "");
                        // Load accepted state from server before rendering
                        fetch("/api/accepted_state")
                            .then(function(res) { return res.ok ? res.json() : {}; })
                            .then(function(acc) {
                                self.state.acceptedTables = (acc && acc.accepted_tables) || {};
                                self.state.acceptedSchemas = (acc && acc.accepted_schemas) || {};
                            })
                            .catch(function() { /* proceed with empty state */ })
                            .then(function() {
                                self.renderers.anomaly(self.state.anomalyData);
                                self.navigate("anomaly");
                            });
                    } else if (data.status === "error") {
                        clearInterval(pollInterval);
                        clearInterval(timer);
                        self.state.abortController = null;
                        alert("Pipeline error: " + (data.error || "Unknown error"));
                        self.navigate("home");
                    }
                    // else status === "running" → keep polling
                })
                .catch(function() { /* ignore transient fetch errors during polling */ });
            }, 3000);
        })
        .catch(function(err) {
            clearInterval(timer);
            self.state.abortController = null;
            if (err.name === "AbortError") return;
            alert("Pipeline error: " + err.message);
            self.navigate("home");
        });
    },

    parsers: {
        anomaly: function(text) {
            return {
                detectors: [],
                report_text: text
            };
        }
    },

    /* ===== SERVER-BASED ACCEPTANCE STATE ===== */

    /**
     * Load accepted tables and schemas from the server's single threshold file.
     * This is called on page load and after each accept action.
     */
    loadAcceptedStateFromServer: function() {
        var self = this;
        fetch("/api/accepted_state")
        .then(function(res) {
            if (!res.ok) throw new Error("Failed to load accepted state");
            return res.json();
        })
        .then(function(data) {
            self.state.acceptedTables = data.accepted_tables || {};
            self.state.acceptedSchemas = data.accepted_schemas || {};
            // Re-render if we are on the anomaly view
            if (self.state.currentView === "anomaly" && self.state.anomalyData) {
                self.renderers.anomaly(self.state.anomalyData);
            }
        })
        .catch(function(err) {
            console.warn("Could not load accepted state from server:", err.message);
        });
    },

    /* ===== FILTERS ===== */

    applyFilters: function() {
        if (this.state.anomalyData) {
            this.renderers.anomaly(this.state.anomalyData);
        }
    },

    getShortName: function(tableName) {
        var value = String(tableName || "");
        if (value.indexOf(".") === -1) return value;
        var parts = value.split(".");
        return parts[parts.length - 1];
    },

    detectorKey: function(detector) {
        return [
            String(detector.category || ""),
            String(detector.detector || ""),
            String(detector.table_name || "")
        ].join("::");
    },

    isAccepted: function(tableName) {
        return !!this.state.acceptedTables[this.getShortName(tableName)];
    },

    isSchemaAccepted: function(tableName) {
        return !!this.state.acceptedSchemas[this.getShortName(tableName)];
    },

    isResolved: function(detector) {
        return !!this.state.resolvedDetectors[this.detectorKey(detector)];
    },

    getRawIssueRows: function(detector) {
        return []
            .concat(detector.monthly_anomalies || [])
            .concat(detector.weekly_anomalies || [])
            .concat(detector.daily_anomalies || []);
    },

    getVisibleIssueRows: function(detector) {
        var rows = this.getRawIssueRows(detector);
        if (this.state.filterPeriod === "all") return rows;
        var visible = [];
        for (var i = 0; i < rows.length; i++) {
            if ((rows[i].period || "").substring(0, 7) === this.state.filterPeriod) {
                visible.push(rows[i]);
            }
        }
        return visible;
    },

    hasAnyIssues: function(detector) {
        if (detector.category === "schema") {
            return this.getUnacceptedSchemaFindings(detector).length > 0;
        }
        return this.getRawIssueRows(detector).length > 0;
    },

    hasVisibleIssues: function(detector) {
        if (detector.category === "schema") {
            return this.getUnacceptedSchemaFindings(detector).length > 0;
        }
        return this.getVisibleIssueRows(detector).length > 0;
    },

    /**
     * For schema detectors, filter out findings that have been accepted
     * UNLESS the data type has changed since acceptance.
     */
    getUnacceptedSchemaFindings: function(detector) {
        var findings = detector.schema_findings || [];
        var shortName = this.getShortName(detector.table_name);
        var acceptedInfo = this.state.acceptedSchemas[shortName];
        if (!acceptedInfo || !acceptedInfo.columns) return findings;

        var result = [];
        for (var i = 0; i < findings.length; i++) {
            var f = findings[i];
            var colName = f.column_name || "";
            var acceptedCol = acceptedInfo.columns[colName];

            if (!acceptedCol) {
                // Not previously accepted — still an issue
                result.push(f);
            } else if (f.data_type && acceptedCol.data_type && f.data_type !== acceptedCol.data_type) {
                // Data type changed since acceptance — re-flag
                result.push(f);
            }
            // Otherwise: column was accepted with same data type → skip
        }
        return result;
    },

    matchesCategoryAndTable: function(detector) {
        if (this.state.filterCategory !== "all" && detector.category !== this.state.filterCategory) return false;
        if (this.state.filterTable !== "all" && detector.table_name !== this.state.filterTable) return false;
        return true;
    },

    matchesFilters: function(detector) {
        if (!this.matchesCategoryAndTable(detector)) return false;
        if (this.state.filterPeriod === "all") return true;
        if (detector.category === "schema") return true;
        return this.getVisibleIssueRows(detector).length > 0;
    },

    getFilteredDetectors: function(detectors) {
        var visible = [];
        for (var i = 0; i < detectors.length; i++) {
            var detector = detectors[i];
            if (this.isResolved(detector)) continue;
            // Skip accepted detectors entirely — they should not show
            if (detector.category === "data" && this.isAccepted(detector.table_name)) continue;
            if (detector.category === "schema" && this.isSchemaAccepted(detector.table_name)
                && this.getUnacceptedSchemaFindings(detector).length === 0) continue;
            if (!this.hasAnyIssues(detector)) continue;
            if (!this.matchesFilters(detector)) continue;
            if (!this.hasVisibleIssues(detector)) continue;
            visible.push(detector);
        }
        return visible;
    },

    computeKpis: function(detectors) {
        var uniqueIssueTables = {};
        var uniqueAcceptedTables = {};
        var dataIssues = 0;
        var schemaIssues = 0;

        for (var i = 0; i < detectors.length; i++) {
            var detector = detectors[i];

            if (detector.category === "data") {
                var issueRows = this.getVisibleIssueRows(detector);
                if (this.isAccepted(detector.table_name)) {
                    uniqueAcceptedTables[detector.table_name] = true;
                    // Accepted data tables don't count toward issues
                } else {
                    uniqueIssueTables[detector.table_name] = true;
                    dataIssues += issueRows.length;
                }
            } else {
                var unaccepted = this.getUnacceptedSchemaFindings(detector);
                if (this.isSchemaAccepted(detector.table_name) && unaccepted.length === 0) {
                    uniqueAcceptedTables[detector.table_name] = true;
                    // Fully accepted schema tables don't count toward issues
                } else {
                    uniqueIssueTables[detector.table_name] = true;
                    schemaIssues += unaccepted.length;
                }
            }
        }

        return {
            tablesWithIssues: Object.keys(uniqueIssueTables).length,
            dataIssues: dataIssues,
            schemaIssues: schemaIssues,
            tablesAccepted: Object.keys(uniqueAcceptedTables).length
        };
    },

    populateFilters: function(detectors) {
        var tableSelect = document.getElementById("filter-table");
        var periodSelect = document.getElementById("period-select");
        var tableMap = {};
        var periodMap = {};
        var availableDetectors = [];
        var currentCategory = this.state.filterCategory;

        for (var i = 0; i < detectors.length; i++) {
            var detector = detectors[i];
            if (this.isResolved(detector) || !this.hasAnyIssues(detector)) continue;
            // Skip accepted detectors from filters too
            if (detector.category === "data" && this.isAccepted(detector.table_name)) continue;
            if (detector.category === "schema" && this.isSchemaAccepted(detector.table_name)
                && this.getUnacceptedSchemaFindings(detector).length === 0) continue;
            if (currentCategory !== "all" && detector.category !== currentCategory) continue;
            availableDetectors.push(detector);
            tableMap[detector.table_name] = true;
        }

        if (this.state.filterTable !== "all" && !tableMap[this.state.filterTable]) {
            this.state.filterTable = "all";
        }

        tableSelect.innerHTML = '<option value="all">All Tables</option>';
        var tables = Object.keys(tableMap).sort();
        for (var t = 0; t < tables.length; t++) {
            tableSelect.innerHTML +=
                '<option value="' + this.esc(tables[t]) + '"' +
                (this.state.filterTable === tables[t] ? " selected" : "") +
                ">" + this.esc(this.getShortName(tables[t])) + "</option>";
        }

        for (var j = 0; j < availableDetectors.length; j++) {
            var detectorForPeriods = availableDetectors[j];
            if (this.state.filterTable !== "all" && detectorForPeriods.table_name !== this.state.filterTable) continue;
            var rows = this.getRawIssueRows(detectorForPeriods);
            for (var k = 0; k < rows.length; k++) {
                periodMap[(rows[k].period || "").substring(0, 7)] = true;
            }
        }

        if (this.state.filterPeriod !== "all" && !periodMap[this.state.filterPeriod]) {
            this.state.filterPeriod = "all";
        }

        periodSelect.innerHTML = '<option value="all">All Periods</option>';
        var periods = Object.keys(periodMap).sort();
        for (var p = 0; p < periods.length; p++) {
            periodSelect.innerHTML +=
                '<option value="' + this.esc(periods[p]) + '"' +
                (this.state.filterPeriod === periods[p] ? " selected" : "") +
                ">" + this.esc(periods[p]) + "</option>";
        }
    },

    /* ===== DATA DETECTOR HELPERS ===== */

    getDataDetectorForTable: function(tableName) {
        var detectors = (this.state.anomalyData && this.state.anomalyData.detectors) || [];
        var shortName = this.getShortName(tableName);
        for (var i = 0; i < detectors.length; i++) {
            if (detectors[i].category !== "data") continue;
            if (this.getShortName(detectors[i].table_name) === shortName) {
                return detectors[i];
            }
        }
        return null;
    },

    getSchemaDetectorForTable: function(tableName) {
        var detectors = (this.state.anomalyData && this.state.anomalyData.detectors) || [];
        var shortName = this.getShortName(tableName);
        for (var i = 0; i < detectors.length; i++) {
            if (detectors[i].category !== "schema") continue;
            if (this.getShortName(detectors[i].table_name) === shortName) {
                return detectors[i];
            }
        }
        return null;
    },

    /* ===== ACCEPT TABLE (DATA ANOMALIES) ===== */

    acceptTable: function(tableName, event) {
        var self = this;
        var detector = this.getDataDetectorForTable(tableName);
        var shortName = this.getShortName(tableName);
        var issueRows = detector ? this.getVisibleIssueRows(detector) : [];
        if (!issueRows.length && detector) {
            issueRows = this.getRawIssueRows(detector);
        }

        // --- Button animation on click ---
        var btn = event && event.currentTarget ? event.currentTarget : null;
        if (btn) {
            btn.classList.add("btn-accept-clicked");
            btn.disabled = true;
            btn.innerHTML = '<span class="btn-spinner"></span> Accepting\u2026';
        }

        // Compute min and max from all issue rows
        var minVal = Infinity;
        var maxVal = -Infinity;
        for (var i = 0; i < issueRows.length; i++) {
            var actual = Number(issueRows[i].actual_value);
            if (isNaN(actual)) continue;
            if (actual > maxVal) maxVal = actual;
            if (actual < minVal) minVal = actual;
        }

        var body = { table_name: shortName };
        if (maxVal !== -Infinity) body.max_val = maxVal;
        if (minVal !== Infinity) body.min_val = minVal;

        fetch("/api/accept_thresholds", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body)
        })
        .then(function(res) {
            if (!res.ok) throw new Error("Threshold update failed");
            return res.json();
        })
        .then(function() {
            self._onAcceptSuccess(shortName, btn, detector);
        })
        .catch(function(err) {
            console.warn("Server accept failed:", err.message);
            alert("Failed to save acceptance. Please ensure the server is running.");
            if (btn) {
                btn.disabled = false;
                btn.classList.remove("btn-accept-clicked");
                btn.innerHTML = 'Accept';
            }
        });
    },

    _onAcceptSuccess: function(shortName, btn, detector) {
        if (btn) {
            btn.innerHTML = '\u2713 Accepted';
            btn.classList.remove("btn-accept-clicked");
            btn.classList.add("btn-accept-done");
        }
        this.state.acceptedTables[shortName] = true;
        if (detector) {
            this.state.resolvedDetectors[this.detectorKey(detector)] = true;
        }
        this.showAcceptOverlay(shortName);
        this.applyFilters();
    },

    /* ===== ACCEPT SCHEMA ===== */

    acceptSchema: function(tableName, event) {
        var self = this;
        var detector = this.getSchemaDetectorForTable(tableName);
        var shortName = this.getShortName(tableName);
        var findings = detector ? (detector.schema_findings || []) : [];

        // --- Button animation ---
        var btn = event && event.currentTarget ? event.currentTarget : null;
        if (btn) {
            btn.classList.add("btn-accept-clicked");
            btn.disabled = true;
            btn.innerHTML = '<span class="btn-spinner"></span> Accepting\u2026';
        }

        // Build accepted findings payload
        var acceptedFindings = [];
        for (var i = 0; i < findings.length; i++) {
            acceptedFindings.push({
                column_name: findings[i].column_name || "",
                finding_type: (findings[i].finding_type || "").replace(/_/g, " "),
                data_type: findings[i].data_type || ""
            });
        }

        var body = {
            table_name: shortName,
            accepted_findings: acceptedFindings
        };

        fetch("/api/accept_schema", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body)
        })
        .then(function(res) {
            if (!res.ok) throw new Error("Schema accept failed");
            return res.json();
        })
        .then(function() {
            self._onSchemaAcceptSuccess(shortName, findings, btn);
        })
        .catch(function(err) {
            console.warn("Server schema accept failed:", err.message);
            alert("Failed to save schema acceptance. Please ensure the server is running.");
            if (btn) {
                btn.disabled = false;
                btn.classList.remove("btn-accept-clicked");
                btn.innerHTML = 'Accept';
            }
        });
    },

    _onSchemaAcceptSuccess: function(shortName, findings, btn) {
        if (btn) {
            btn.innerHTML = '\u2713 Accepted';
            btn.classList.remove("btn-accept-clicked");
            btn.classList.add("btn-accept-done");
        }

        // Store accepted columns with their data types
        var columns = {};
        for (var i = 0; i < findings.length; i++) {
            var colName = findings[i].column_name || "";
            if (colName) {
                columns[colName] = {
                    finding_type: findings[i].finding_type || "",
                    data_type: findings[i].data_type || "",
                };
            }
        }

        // Merge with existing accepted columns (in-memory for immediate UI update)
        if (!this.state.acceptedSchemas[shortName]) {
            this.state.acceptedSchemas[shortName] = { columns: {} };
        }
        var existing = this.state.acceptedSchemas[shortName].columns || {};
        for (var key in columns) {
            if (columns.hasOwnProperty(key)) {
                existing[key] = columns[key];
            }
        }
        this.state.acceptedSchemas[shortName].columns = existing;

        this.showAcceptOverlay(shortName + " (Schema)");
        this.applyFilters();
    },

    /* ===== REVIEW ===== */

    reviewDetector: function(tableName) {
        var shortName = this.getShortName(tableName);
        var url = this.REVIEW_URL + "?table=" + encodeURIComponent(shortName);
        window.open(url, "_blank", "noopener");
    },

    /* ===== ACCEPT OVERLAY ===== */

    showAcceptOverlay: function(tableName) {
        var overlay = document.getElementById("accept-overlay");
        var msg = document.getElementById("accept-overlay-msg");
        if (!overlay || !msg) {
            this.showToast("Accepted " + tableName);
            return;
        }

        msg.innerText = '"' + tableName + '" has been accepted and the local max anomaly threshold has been saved.';
        overlay.style.display = "flex";

        var tile = overlay.querySelector(".accept-tile");
        overlay.style.animation = "none";
        void overlay.offsetWidth;
        overlay.style.animation = "";

        if (tile) {
            tile.style.animation = "none";
            void tile.offsetWidth;
            tile.style.animation = "";
        }

        var circle = overlay.querySelector(".check-circle");
        var path = overlay.querySelector(".check-path");
        if (circle) {
            circle.style.animation = "none";
            void circle.offsetWidth;
            circle.style.animation = "";
        }
        if (path) {
            path.style.animation = "none";
            void path.offsetWidth;
            path.style.animation = "";
        }

        var sparkles = overlay.querySelectorAll(".sparkle");
        for (var i = 0; i < sparkles.length; i++) {
            sparkles[i].style.animation = "none";
            void sparkles[i].offsetWidth;
            sparkles[i].style.animation = "";
        }

        if (this._overlayTimer) clearTimeout(this._overlayTimer);
        var self = this;
        this._overlayTimer = setTimeout(function() {
            self.closeAcceptOverlay();
        }, 3500);
    },

    closeAcceptOverlay: function() {
        var overlay = document.getElementById("accept-overlay");
        if (!overlay || overlay.style.display === "none") return;

        var tile = overlay.querySelector(".accept-tile");
        if (tile) tile.style.animation = "tileExit 0.4s ease forwards";
        overlay.style.animation = "overlayFadeOut 0.4s ease forwards";

        setTimeout(function() {
            overlay.style.display = "none";
            overlay.style.animation = "";
            if (tile) tile.style.animation = "";
        }, 450);
    },

    showToast: function(message) {
        var toast = document.getElementById("toast");
        if (!toast) return;
        document.getElementById("toast-msg").innerText = message;
        toast.style.display = "flex";
        void toast.offsetWidth;
        toast.classList.add("show");
        if (this._toastTimer) clearTimeout(this._toastTimer);
        this._toastTimer = setTimeout(function() {
            toast.classList.remove("show");
            setTimeout(function() {
                toast.style.display = "none";
            }, 300);
        }, 2600);
    },

    /* ===== RENDERERS ===== */

    renderers: {
        anomaly: function(data) {
            var detectors = data.detectors || [];
            app.populateFilters(detectors);

            var filtered = app.getFilteredDetectors(detectors);
            var kpis = app.computeKpis(detectors);

            document.getElementById("tables-with-issues").innerText = kpis.tablesWithIssues;
            document.getElementById("data-issues").innerText = kpis.dataIssues;
            document.getElementById("schema-issues").innerText = kpis.schemaIssues;
            document.getElementById("tables-accepted").innerText = kpis.tablesAccepted;

            var container = document.getElementById("tables-container");
            container.innerHTML = "";

            if (!filtered.length) {
                container.innerHTML =
                    '<div class="content-card empty-state">No anomaly issues match the selected filters.</div>';
                return;
            }

            for (var i = 0; i < filtered.length; i++) {
                container.innerHTML += app.renderDetectorCard(filtered[i], i);
            }
        }
    },

    /* ===== CARD RENDERING ===== */

    renderDetectorCard: function(detector, index) {
        var shortName = this.getShortName(detector.table_name || "");
        var description = this.TABLE_DESCRIPTIONS[shortName] || "Monitors anomalous behavior and structural changes for this table.";
        var accepted = (detector.category === "data")
            ? this.isAccepted(shortName)
            : this.isSchemaAccepted(shortName);

        var html = "";
        html += '<div class="table-card ' + (accepted ? "accepted " : "") + "category-" + detector.category + '" style="animation-delay:' + (index * 70) + 'ms;">';
        html += '<div class="tbl-hdr">';
        html += '<div class="tbl-main">';
        html += '<div class="tbl-name">' + this.esc(detector.display_name) + '</div>';
        html += '<div class="tbl-description">' + this.esc(description) + '</div>';
        html += '<div class="tbl-meta-row">';
        html += '<span class="category-badge category-' + this.esc(detector.category) + '">' + this.esc(detector.category.toUpperCase()) + "</span>";
        html += '<span class="status-badge anomaly">Issues Detected</span>';
        html += "</div>";
        html += "</div>";
        html += '<div class="tbl-actions">';

        if (detector.category === "data") {
            if (!accepted) {
                html += '<button class="btn btn-accept" onclick="app.acceptTable(' + this.jsArg(shortName) + ', event)">Accept</button>';
            } else {
                html += '<span class="accepted-label">\u2713 Accepted</span>';
            }
            html += '<button class="btn btn-review" onclick="app.reviewDetector(' + this.jsArg(shortName) + ')">Review</button>';
        } else {
            // Schema category — also gets Accept + Review buttons
            if (!accepted) {
                html += '<button class="btn btn-accept" onclick="app.acceptSchema(' + this.jsArg(shortName) + ', event)">Accept</button>';
            } else {
                html += '<span class="accepted-label">\u2713 Accepted</span>';
            }
            html += '<button class="btn btn-review" onclick="app.reviewDetector(' + this.jsArg(shortName) + ')">Review</button>';
        }

        html += "</div></div>";
        html += '<div class="tbl-body">';
        html += '<p class="helper-text"><strong>Table:</strong> ' + this.esc(detector.table_fqn) + "</p>";

        if (detector.category === "data") {
            html += this.renderDataSections(detector);
        } else {
            html += this.renderSchemaSections(detector);
        }

        html += "</div></div>";
        return html;
    },

    renderDataSections: function(detector) {
        var html = "";
        html += this.renderDataTable("Monthly", detector.monthly_anomalies || []);
        html += this.renderDataTable("Weekly", detector.weekly_anomalies || []);
        html += this.renderDataTable("Daily", detector.daily_anomalies || []);
        return html;
    },

    renderDataTable: function(label, rows) {
        var visibleRows = [];
        for (var i = 0; i < rows.length; i++) {
            if (this.state.filterPeriod === "all" || (rows[i].period || "").substring(0, 7) === this.state.filterPeriod) {
                visibleRows.push(rows[i]);
            }
        }
        if (!visibleRows.length) return "";

        var html = '<div class="granularity-label">' + this.esc(label) + ' Anomalies</div>';
        html += '<table class="data-table"><thead><tr><th>Period</th><th>Value</th><th>Expected Range</th><th>Status</th></tr></thead><tbody>';
        for (var i = 0; i < visibleRows.length; i++) {
            var row = visibleRows[i];
            var statusClass = String(row.direction || "").indexOf("Higher") !== -1 ? "dir-hi" : "dir-lo";
            var range = this.formatNumber(row.lower_bound) + " - " + this.formatNumber(row.upper_bound);
            html += "<tr>";
            html += "<td>" + this.esc(row.period + (row.group_label ? " [" + row.group_label + "]" : "")) + "</td>";
            html += '<td class="col-r">' + this.esc(this.formatNumber(row.actual_value)) + "</td>";
            html += '<td class="col-r">' + this.esc(range) + "</td>";
            html += '<td class="' + statusClass + '">' + this.esc(row.direction) + "</td>";
            html += "</tr>";
        }
        html += "</tbody></table>";
        return html;
    },

    renderSchemaSections: function(detector) {
        // Only show unaccepted schema findings
        var findings = this.getUnacceptedSchemaFindings(detector);
        if (!findings.length) {
            return '<div class="helper-text">No schema anomalies found for this table.</div>';
        }

        var html = '<div class="granularity-label">Schema Findings</div><div class="finding-list">';
        for (var i = 0; i < findings.length; i++) {
            var columnName = findings[i].column_name || "";
            var dataType = findings[i].data_type || "";
            html += '<div class="finding-item">';
            html += '<div class="finding-title">' + this.esc(findings[i].finding_type.replace(/_/g, " ")) + "</div>";
            if (columnName) {
                html += '<div class="finding-column">Column: ' + this.esc(columnName) + "</div>";
            }
            if (dataType) {
                html += '<div class="finding-column" style="margin-left:6px;">Type: ' + this.esc(dataType) + "</div>";
            }
            html += '<div class="finding-body">' + this.esc(findings[i].details) + "</div>";
            html += "</div>";
        }
        html += "</div>";
        return html;
    },

    formatNumber: function(value) {
        var num = Number(value);
        if (isNaN(num)) return String(value);
        return num.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });
    },

    esc: function(value) {
        if (value === null || value === undefined) return "";
        return String(value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    },

    jsArg: function(value) {
        var s = String(value == null ? "" : value).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
        return "'" + s + "'";
    }
};

window.app = app;
// Load accepted state from the server on page load
app.loadAcceptedStateFromServer();
