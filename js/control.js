/**
 * Truck & Tractor Pulling Control Panel - Unified Controller
 * Designed to run seamlessly in both file:// (local Explorer) and http(s):// (GitHub Pages)
 * without ES module CORS restrictions.
 */

(function () {
  'use strict';

  // ==========================================================================
  // 1. Storage Manager (localStorage with graceful in-memory fallback)
  // ==========================================================================
  const STORAGE_KEYS = {
    CHANNEL_KEY: 'pulling_channel_key',
    BROKER_URL: 'pulling_broker_url',
    EVENT_INFO: 'pulling_event_info',
    ANNOUNCER_INFO: 'pulling_announcer_info',
    MISC_INFO: 'pulling_misc_info',
    PARTICIPANTS_RAW: 'pulling_participants_raw',
    PARTICIPANTS_LIST: 'pulling_participants_list',
    SELECTED_CLASS: 'pulling_selected_class',
    CUSTOM_CLASS: 'pulling_custom_class',
    RESULTS_MODE: 'pulling_results_mode',
    BYPASS_RAW: 'pulling_bypass_raw',
    BYPASS_ACTIVE: 'pulling_bypass_active'
  };

  const memoryFallback = {};

  const Storage = {
    save(key, value) {
      try {
        localStorage.setItem(key, JSON.stringify(value));
      } catch (e) {
        memoryFallback[key] = JSON.stringify(value);
      }
    },

    load(key, defaultValue = null) {
      try {
        const item = localStorage.getItem(key);
        return item !== null ? JSON.parse(item) : defaultValue;
      } catch (e) {
        const fallbackItem = memoryFallback[key];
        return fallbackItem !== undefined ? JSON.parse(fallbackItem) : defaultValue;
      }
    },

    remove(key) {
      try {
        localStorage.removeItem(key);
      } catch (e) {
        delete memoryFallback[key];
      }
    }
  };

  // ==========================================================================
  // 2. MQTT Service Manager
  // ==========================================================================
  class MqttService {
    constructor() {
      this.client = null;
      this.channelKey = '';
      this.brokerUrl = 'wss://broker.hivemq.com:8884/mqtt';
      this.statusCallback = null;
      this.isConnected = false;
    }

    setBrokerUrl(url) {
      if (url && url.trim()) {
        this.brokerUrl = url.trim();
      }
    }

    connect(channelKey, onStatusChange) {
      this.channelKey = (channelKey || '').trim();
      this.statusCallback = onStatusChange;

      if (!this.channelKey) {
        this._updateStatus('disconnected', 'Set a channel key');
        return;
      }

      if (typeof window.mqtt === 'undefined') {
        this._updateStatus('connecting', 'Waiting for MQTT library...');
        console.warn('MQTT library is not yet loaded or blocked by network.');
        // Retry shortly if CDN is still downloading
        setTimeout(() => {
          if (typeof window.mqtt !== 'undefined' && !this.isConnected) {
            this.connect(this.channelKey, this.statusCallback);
          } else if (typeof window.mqtt === 'undefined') {
            this._updateStatus('error', 'MQTT CDN blocked or offline');
          }
        }, 2000);
        return;
      }

      if (this.client) {
        try {
          this.client.end(true);
        } catch (e) {}
      }

      this._updateStatus('connecting', 'Connecting to broker...');

      const clientId = `pulling_ctrl_${Math.random().toString(16).substring(2, 10)}`;

      try {
        this.client = window.mqtt.connect(this.brokerUrl, {
          clientId,
          clean: true,
          connectTimeout: 8000,
          reconnectPeriod: 3000,
          keepalive: 30
        });

        this.client.on('connect', () => {
          this.isConnected = true;
          this._updateStatus('connected', 'Live & Connected');
          console.log(`MQTT connected to ${this.brokerUrl} [${this.channelKey}]`);
        });

        this.client.on('reconnect', () => {
          this.isConnected = false;
          this._updateStatus('connecting', 'Reconnecting...');
        });

        this.client.on('close', () => {
          this.isConnected = false;
          this._updateStatus('disconnected', 'Disconnected');
        });

        this.client.on('error', (err) => {
          console.error('MQTT error:', err);
          this._updateStatus('error', 'Broker error');
        });

        this.client.on('offline', () => {
          this.isConnected = false;
          this._updateStatus('offline', 'Offline');
        });
      } catch (err) {
        console.error('Failed to create MQTT client:', err);
        this._updateStatus('error', 'Connection failed');
      }
    }

    disconnect() {
      if (this.client) {
        try {
          this.client.end();
        } catch (e) {}
        this.client = null;
      }
      this.isConnected = false;
      this._updateStatus('disconnected', 'Disconnected');
    }

    publishState(statePayload) {
      if (!this.client || !this.isConnected) {
        console.log('State updated locally (MQTT not connected yet):', statePayload.activeOverlay);
        return false;
      }

      const topic = `pulling-livestream/${this.channelKey}/state`;
      const message = JSON.stringify({
        ...statePayload,
        timestamp: Date.now()
      });

      this.client.publish(topic, message, { qos: 1, retain: true }, (err) => {
        if (err) {
          console.error('Publish error:', err);
        } else {
          console.log(`Published state to ${topic}:`, statePayload.activeOverlay);
        }
      });
      return true;
    }

    _updateStatus(stateName, message) {
      if (typeof this.statusCallback === 'function') {
        this.statusCallback({ state: stateName, message });
      }
    }
  }

  // ==========================================================================
  // 3. Application State & Variables
  // ==========================================================================
  const mqttService = new MqttService();

  const appState = {
    channelKey: '',
    brokerUrl: 'wss://broker.hivemq.com:8884/mqtt',
    eventInfo: { heading: '', sub: '' },
    announcerInfo: { heading: '', sub: '' },
    miscInfo: { heading: '', sub: '' },
    participantsRaw: '',
    participants: [], // [{ id, vehicle, driver, hometown, distance, position }]
    selectedClass: 'AP1 Two Wheel Drive Trucks',
    customClass: '',
    resultsMode: 'Current Standings',
    bypassRaw: '',
    bypassActive: false,
    bypassItems: []
  };

  let activeTimer = null;

  // ==========================================================================
  // 4. App Initialization
  // ==========================================================================
  function init() {
    loadSavedState();
    setupEventListeners();
    renderParticipantsList();
    updateStandingsPreview();

    if (appState.channelKey) {
      connectMqttClient();
    }
  }

  function loadSavedState() {
    appState.channelKey = Storage.load(STORAGE_KEYS.CHANNEL_KEY, '') || '';
    appState.brokerUrl = Storage.load(STORAGE_KEYS.BROKER_URL, 'wss://broker.hivemq.com:8884/mqtt') || 'wss://broker.hivemq.com:8884/mqtt';
    appState.eventInfo = Storage.load(STORAGE_KEYS.EVENT_INFO, { heading: '', sub: '' });
    appState.announcerInfo = Storage.load(STORAGE_KEYS.ANNOUNCER_INFO, { heading: '', sub: '' });
    appState.miscInfo = Storage.load(STORAGE_KEYS.MISC_INFO, { heading: '', sub: '' });
    appState.participantsRaw = Storage.load(STORAGE_KEYS.PARTICIPANTS_RAW, '') || '';
    appState.participants = Storage.load(STORAGE_KEYS.PARTICIPANTS_LIST, []) || [];
    appState.selectedClass = Storage.load(STORAGE_KEYS.SELECTED_CLASS, 'AP1 Two Wheel Drive Trucks');
    appState.customClass = Storage.load(STORAGE_KEYS.CUSTOM_CLASS, '') || '';
    appState.resultsMode = Storage.load(STORAGE_KEYS.RESULTS_MODE, 'Current Standings') || 'Current Standings';
    appState.bypassRaw = Storage.load(STORAGE_KEYS.BYPASS_RAW, '') || '';
    appState.bypassActive = Storage.load(STORAGE_KEYS.BYPASS_ACTIVE, false) || false;

    // DOM bindings
    const channelInput = document.getElementById('channelKeyInput');
    if (channelInput) channelInput.value = appState.channelKey;

    const brokerInput = document.getElementById('brokerUrlInput');
    if (brokerInput) brokerInput.value = appState.brokerUrl;

    const eventH = document.getElementById('eventHeading');
    const eventS = document.getElementById('eventSub');
    if (eventH) eventH.value = appState.eventInfo.heading || '';
    if (eventS) eventS.value = appState.eventInfo.sub || '';

    const annH = document.getElementById('announcerHeading');
    const annS = document.getElementById('announcerSub');
    if (annH) annH.value = appState.announcerInfo.heading || '';
    if (annS) annS.value = appState.announcerInfo.sub || '';

    const misH = document.getElementById('miscHeading');
    const misS = document.getElementById('miscSub');
    if (misH) misH.value = appState.miscInfo.heading || '';
    if (misS) misS.value = appState.miscInfo.sub || '';

    const tsvInput = document.getElementById('tsvParticipantsInput');
    if (tsvInput) tsvInput.value = appState.participantsRaw || '';

    const classSelect = document.getElementById('classSelect');
    const manualClassInput = document.getElementById('manualClassInput');
    const manualClassWrapper = document.getElementById('manualClassWrapper');

    if (classSelect) {
      if (Array.from(classSelect.options).some(o => o.value === appState.selectedClass)) {
        classSelect.value = appState.selectedClass;
      } else {
        classSelect.value = '__manual__';
      }
    }
    if (manualClassInput) manualClassInput.value = appState.customClass || '';
    if (manualClassWrapper && classSelect) {
      manualClassWrapper.style.display = classSelect.value === '__manual__' ? 'block' : 'none';
    }

    setModeUI(appState.resultsMode);

    const bypassInput = document.getElementById('tsvBypassInput');
    if (bypassInput) bypassInput.value = appState.bypassRaw || '';

    if (appState.bypassActive && appState.bypassRaw) {
      appState.bypassItems = parseBypassTSV(appState.bypassRaw);
      updateOverrideBanner(true);
    }

    updateCopyButtonState();
  }

  // ==========================================================================
  // 5. Event Listeners Setup
  // ==========================================================================
  function setupEventListeners() {
    // 5.1: Channel Key Panel
    const btnRandomKey = document.getElementById('btnRandomKey');
    const btnSetKey = document.getElementById('btnSetKey');
    const btnCopyUrl = document.getElementById('btnCopyUrl');
    const channelKeyInput = document.getElementById('channelKeyInput');
    const toggleBrokerConfig = document.getElementById('toggleBrokerConfig');
    const brokerConfigBox = document.getElementById('brokerConfigBox');
    const brokerUrlInput = document.getElementById('brokerUrlInput');

    if (btnRandomKey && channelKeyInput) {
      btnRandomKey.addEventListener('click', () => {
        const randomKey = 'pull-' + Math.random().toString(36).substring(2, 7);
        channelKeyInput.value = randomKey;
        showToast(`Generated: ${randomKey}`, 'info');
      });
    }

    if (btnSetKey && channelKeyInput) {
      btnSetKey.addEventListener('click', () => {
        const key = channelKeyInput.value.trim();
        if (!key) {
          showToast('Please enter a channel key', 'warning');
          channelKeyInput.focus();
          return;
        }
        appState.channelKey = key;
        Storage.save(STORAGE_KEYS.CHANNEL_KEY, key);

        if (brokerUrlInput) {
          appState.brokerUrl = brokerUrlInput.value.trim() || appState.brokerUrl;
          Storage.save(STORAGE_KEYS.BROKER_URL, appState.brokerUrl);
        }

        updateCopyButtonState();
        connectMqttClient();
        showToast(`Channel key saved: "${key}"`, 'success');
      });
    }

    if (btnCopyUrl) {
      btnCopyUrl.addEventListener('click', copyOverlayUrl);
    }

    if (toggleBrokerConfig && brokerConfigBox) {
      toggleBrokerConfig.addEventListener('click', () => {
        brokerConfigBox.classList.toggle('open');
      });
    }

    // 5.2: Event Info Persistent Inputs
    const attachInputSaver = (id, parentObj, prop, storageKey) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener('input', () => {
        appState[parentObj][prop] = el.value;
        Storage.save(storageKey, appState[parentObj]);
      });
    };

    attachInputSaver('eventHeading', 'eventInfo', 'heading', STORAGE_KEYS.EVENT_INFO);
    attachInputSaver('eventSub', 'eventInfo', 'sub', STORAGE_KEYS.EVENT_INFO);
    attachInputSaver('announcerHeading', 'announcerInfo', 'heading', STORAGE_KEYS.ANNOUNCER_INFO);
    attachInputSaver('announcerSub', 'announcerInfo', 'sub', STORAGE_KEYS.ANNOUNCER_INFO);
    attachInputSaver('miscHeading', 'miscInfo', 'heading', STORAGE_KEYS.MISC_INFO);
    attachInputSaver('miscSub', 'miscInfo', 'sub', STORAGE_KEYS.MISC_INFO);

    // Event Info Sliders (15s timers)
    bindCountdownSlider({
      toggleId: 'toggleEventInfo',
      rowId: 'rowEventInfo',
      meterId: 'meterEventInfo',
      timerTagId: 'timerEventInfo',
      durationSeconds: 15,
      getPayload: () => ({
        activeOverlay: 'event_info',
        overlayData: {
          type: 'event_info',
          heading: appState.eventInfo.heading || 'Truck & Tractor Pulling',
          subText: appState.eventInfo.sub || ''
        }
      })
    });

    bindCountdownSlider({
      toggleId: 'toggleAnnouncerInfo',
      rowId: 'rowAnnouncerInfo',
      meterId: 'meterAnnouncerInfo',
      timerTagId: 'timerAnnouncerInfo',
      durationSeconds: 15,
      getPayload: () => ({
        activeOverlay: 'announcer_info',
        overlayData: {
          type: 'announcer_info',
          heading: appState.announcerInfo.heading || 'Track Announcer',
          subText: appState.announcerInfo.sub || ''
        }
      })
    });

    bindCountdownSlider({
      toggleId: 'toggleMiscInfo',
      rowId: 'rowMiscInfo',
      meterId: 'meterMiscInfo',
      timerTagId: 'timerMiscInfo',
      durationSeconds: 15,
      getPayload: () => ({
        activeOverlay: 'misc_info',
        overlayData: {
          type: 'misc_info',
          heading: appState.miscInfo.heading || 'Track Notice',
          subText: appState.miscInfo.sub || ''
        }
      })
    });

    // 5.3: Participants Panel
    const tsvInput = document.getElementById('tsvParticipantsInput');
    const btnParse = document.getElementById('btnParseParticipants');
    const btnClearData = document.getElementById('btnClearData');
    const btnAddManual = document.getElementById('btnAddManualParticipant');

    if (tsvInput) {
      tsvInput.addEventListener('input', () => {
        appState.participantsRaw = tsvInput.value;
        Storage.save(STORAGE_KEYS.PARTICIPANTS_RAW, appState.participantsRaw);
      });
    }

    if (btnParse && tsvInput) {
      btnParse.addEventListener('click', () => {
        const text = tsvInput.value.trim();
        if (!text) {
          showToast('Paste Google Sheets cells into the box first', 'warning');
          return;
        }
        parseAndSaveParticipants(text);
      });
    }

    if (btnClearData) {
      btnClearData.addEventListener('click', () => {
        if (appState.participants.length > 0 && !confirm('Clear all participant data and the import box?')) {
          return;
        }
        if (tsvInput) tsvInput.value = '';
        appState.participantsRaw = '';
        appState.participants = [];
        Storage.save(STORAGE_KEYS.PARTICIPANTS_RAW, '');
        Storage.save(STORAGE_KEYS.PARTICIPANTS_LIST, []);
        renderParticipantsList();
        updateStandingsPreview();
        showToast('Participant data cleared', 'info');
      });
    }

    if (btnAddManual) {
      btnAddManual.addEventListener('click', handleManualParticipantAdd);
    }

    // 5.4: Results & Standings Panel
    const classSelect = document.getElementById('classSelect');
    const manualClassInput = document.getElementById('manualClassInput');
    const manualClassWrapper = document.getElementById('manualClassWrapper');
    const btnModeStandings = document.getElementById('btnModeStandings');
    const btnModeFinal = document.getElementById('btnModeFinal');
    const btnActivateOverride = document.getElementById('btnActivateOverride');
    const btnDeactivateOverride = document.getElementById('btnDeactivateOverride');
    const btnClearBypass = document.getElementById('btnClearBypass');
    const tsvBypassInput = document.getElementById('tsvBypassInput');

    if (classSelect) {
      classSelect.addEventListener('change', () => {
        appState.selectedClass = classSelect.value;
        Storage.save(STORAGE_KEYS.SELECTED_CLASS, appState.selectedClass);
        if (classSelect.value === '__manual__') {
          if (manualClassWrapper) manualClassWrapper.style.display = 'block';
          if (manualClassInput) manualClassInput.focus();
        } else {
          if (manualClassWrapper) manualClassWrapper.style.display = 'none';
        }
        updateResultsTitle();
      });
    }

    if (manualClassInput) {
      manualClassInput.addEventListener('input', () => {
        appState.customClass = manualClassInput.value;
        Storage.save(STORAGE_KEYS.CUSTOM_CLASS, appState.customClass);
        updateResultsTitle();
      });
    }

    if (btnModeStandings) {
      btnModeStandings.addEventListener('click', () => {
        setModeUI('Current Standings');
        updateResultsTitle();
      });
    }

    if (btnModeFinal) {
      btnModeFinal.addEventListener('click', () => {
        setModeUI('Final Results');
        updateResultsTitle();
      });
    }

    // Results 20s Overlay Slider (with Class & Mode Confirmation Prompt)
    bindCountdownSlider({
      toggleId: 'toggleResultsOverlay',
      rowId: 'rowResultsOverlay',
      meterId: 'meterResultsOverlay',
      timerTagId: 'timerResultsOverlay',
      durationSeconds: 20,
      onBeforeActivate: (onConfirm, onCancel) => {
        const modal = document.getElementById('resultsConfirmModal');
        const modalClass = document.getElementById('confirmModalClass');
        const modalMode = document.getElementById('confirmModalMode');
        const btnYes = document.getElementById('btnConfirmResultsYes');
        const btnNo = document.getElementById('btnConfirmResultsNo');

        const className = getEffectiveClassName();
        const mode = appState.resultsMode;

        if (!modal) {
          if (confirm(`Are the class and mode correct?\n\nClass: ${className}\nMode: ${mode}`)) {
            onConfirm();
          } else {
            onCancel();
          }
          return;
        }

        if (modalClass) modalClass.textContent = className;
        if (modalMode) modalMode.textContent = mode;

        let cleanup = null;

        const closeModal = () => {
          modal.classList.remove('is-open');
          setTimeout(() => {
            modal.style.display = 'none';
          }, 200);
          if (cleanup) cleanup();
        };

        const handleYes = (e) => {
          e.preventDefault();
          closeModal();
          onConfirm();
        };

        const handleNo = (e) => {
          e.preventDefault();
          closeModal();
          onCancel();
        };

        const handleBackdrop = (e) => {
          if (e.target === modal) {
            handleNo(e);
          }
        };

        const handleKeydown = (e) => {
          if (e.key === 'Escape') {
            handleNo(e);
          }
        };

        cleanup = () => {
          if (btnYes) btnYes.removeEventListener('click', handleYes);
          if (btnNo) btnNo.removeEventListener('click', handleNo);
          modal.removeEventListener('click', handleBackdrop);
          document.removeEventListener('keydown', handleKeydown);
        };

        if (btnYes) btnYes.addEventListener('click', handleYes);
        if (btnNo) btnNo.addEventListener('click', handleNo);
        modal.addEventListener('click', handleBackdrop);
        document.addEventListener('keydown', handleKeydown);

        modal.style.display = 'flex';
        void modal.offsetWidth;
        modal.classList.add('is-open');
      },
      getPayload: () => {
        const items = appState.bypassActive ? appState.bypassItems : getCompiledStandings();
        return {
          activeOverlay: 'results',
          overlayData: {
            type: 'results_table',
            title: getEffectiveResultsTitle(),
            mode: appState.resultsMode,
            className: getEffectiveClassName(),
            isOverride: appState.bypassActive,
            items: items
          }
        };
      }
    });

    if (tsvBypassInput) {
      tsvBypassInput.addEventListener('input', () => {
        appState.bypassRaw = tsvBypassInput.value;
        Storage.save(STORAGE_KEYS.BYPASS_RAW, appState.bypassRaw);
      });
    }

    if (btnActivateOverride) {
      btnActivateOverride.addEventListener('click', () => {
        const raw = tsvBypassInput ? tsvBypassInput.value.trim() : '';
        if (!raw) {
          showToast('Paste TSV bypass rows first', 'warning');
          return;
        }
        const parsed = parseBypassTSV(raw);
        if (parsed.length === 0) {
          showToast('Could not parse valid rows from bypass box', 'error');
          return;
        }
        appState.bypassItems = parsed;
        appState.bypassActive = true;
        Storage.save(STORAGE_KEYS.BYPASS_ACTIVE, true);
        updateOverrideBanner(true);
        updateStandingsPreview();
        showToast(`Override activated with ${parsed.length} rows`, 'success');

        // If results overlay currently active, broadcast update
        const toggleResults = document.getElementById('toggleResultsOverlay');
        if (toggleResults && toggleResults.checked) {
          broadcastResultsState();
        }
      });
    }

    if (btnDeactivateOverride) {
      btnDeactivateOverride.addEventListener('click', () => {
        appState.bypassActive = false;
        Storage.save(STORAGE_KEYS.BYPASS_ACTIVE, false);
        updateOverrideBanner(false);
        updateStandingsPreview();
        showToast('Reverted to auto-compiled standings', 'info');

        const toggleResults = document.getElementById('toggleResultsOverlay');
        if (toggleResults && toggleResults.checked) {
          broadcastResultsState();
        }
      });
    }

    if (btnClearBypass && tsvBypassInput) {
      btnClearBypass.addEventListener('click', () => {
        tsvBypassInput.value = '';
        appState.bypassRaw = '';
        Storage.save(STORAGE_KEYS.BYPASS_RAW, '');
        showToast('Bypass input cleared', 'info');
      });
    }
  }

  // ==========================================================================
  // 6. Network / MQTT Functions
  // ==========================================================================
  function connectMqttClient() {
    mqttService.setBrokerUrl(appState.brokerUrl);
    mqttService.connect(appState.channelKey, ({ state, message }) => {
      const badge = document.getElementById('connectionBadge');
      const text = document.getElementById('connectionText');
      if (badge && text) {
        badge.className = `status-badge ${state}`;
        text.textContent = message;
      }
    });
  }

  function updateCopyButtonState() {
    const btnCopyUrl = document.getElementById('btnCopyUrl');
    if (!btnCopyUrl) return;

    if (appState.channelKey) {
      btnCopyUrl.disabled = false;
      btnCopyUrl.title = 'Copy overlay URL with this channel key';
    } else {
      btnCopyUrl.disabled = true;
      btnCopyUrl.title = 'Set a channel key first';
    }
  }

  function copyOverlayUrl() {
    if (!appState.channelKey) return;

    let overlayUrl = '';
    if (window.location.protocol === 'file:') {
      // Local file preview fallback
      const pathname = window.location.href;
      overlayUrl = pathname.replace(/index\.html$/i, 'overlay.html') + `?key=${encodeURIComponent(appState.channelKey)}`;
    } else {
      const currentPath = window.location.pathname;
      const basePath = currentPath.substring(0, currentPath.lastIndexOf('/') + 1);
      overlayUrl = `${window.location.origin}${basePath}overlay.html?key=${encodeURIComponent(appState.channelKey)}`;
    }

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(overlayUrl).then(() => {
        showToast('Overlay URL copied to clipboard!', 'success');
      }).catch(() => {
        prompt('Copy Overlay URL:', overlayUrl);
      });
    } else {
      prompt('Copy Overlay URL:', overlayUrl);
    }
  }

  // ==========================================================================
  // 7. Participant List Operations
  // ==========================================================================
  function parseAndSaveParticipants(tsvText) {
    const lines = tsvText.split(/\r?\n/);
    const newParticipants = [];

    const existingDistances = new Map();
    appState.participants.forEach(p => {
      const k = `${(p.vehicle || '').toLowerCase()}|${(p.driver || '').toLowerCase()}`;
      if (p.distance) existingDistances.set(k, p.distance);
    });

    lines.forEach((line, index) => {
      const trimmed = line.trim();
      if (!trimmed) return;

      const parts = line.split('\t').map(s => s.trim());
      const vehicle = parts[0] || '';
      const driver = parts[1] || '';
      const hometown = parts[2] || '';

      // Skip header row if present
      if (index === 0 && (
        vehicle.toLowerCase() === 'vehicle' ||
        driver.toLowerCase() === 'driver' ||
        hometown.toLowerCase() === 'hometown'
      )) {
        return;
      }

      if (!vehicle && !driver) return;

      const k = `${vehicle.toLowerCase()}|${driver.toLowerCase()}`;
      const preservedDistance = existingDistances.get(k) || '';

      newParticipants.push({
        id: `p_${Date.now()}_${index}`,
        vehicle,
        driver,
        hometown,
        distance: preservedDistance
      });
    });

    appState.participants = newParticipants;
    appState.participantsRaw = tsvText;
    Storage.save(STORAGE_KEYS.PARTICIPANTS_RAW, tsvText);
    Storage.save(STORAGE_KEYS.PARTICIPANTS_LIST, newParticipants);

    renderParticipantsList();
    updateStandingsPreview();
    showToast(`Loaded ${newParticipants.length} participants`, 'success');
  }

  function handleManualParticipantAdd() {
    const vInput = document.getElementById('manualVehicle');
    const dInput = document.getElementById('manualDriver');
    const hInput = document.getElementById('manualHometown');

    const vehicle = (vInput ? vInput.value : '').trim();
    const driver = (dInput ? dInput.value : '').trim();
    const hometown = (hInput ? hInput.value : '').trim();

    if (!vehicle && !driver) {
      showToast('Enter at least a vehicle or driver name', 'warning');
      return;
    }

    const newP = {
      id: `p_man_${Date.now()}`,
      vehicle: vehicle || 'Unknown Vehicle',
      driver: driver || 'Unknown Driver',
      hometown: hometown || '',
      distance: ''
    };

    appState.participants.push(newP);
    Storage.save(STORAGE_KEYS.PARTICIPANTS_LIST, appState.participants);

    if (vInput) vInput.value = '';
    if (dInput) dInput.value = '';
    if (hInput) hInput.value = '';

    renderParticipantsList();
    updateStandingsPreview();
    showToast(`Added ${newP.vehicle}`, 'success');
  }

  function calculatePositions() {
    const scored = appState.participants
      .filter(p => p.distance && !isNaN(parseFloat(p.distance)) && parseFloat(p.distance) > 0)
      .sort((a, b) => parseFloat(b.distance) - parseFloat(a.distance));

    appState.participants.forEach(p => p.position = null);

    scored.forEach((p, idx) => {
      p.position = idx + 1;
    });
  }

  function renderParticipantsList() {
    const container = document.getElementById('participantsList');
    const countEl = document.getElementById('participantCount');
    if (!container) return;

    if (countEl) countEl.textContent = appState.participants.length;

    if (appState.participants.length === 0) {
      container.innerHTML = `
        <div style="text-align: center; padding: 2rem; color: var(--text-muted); font-size: 0.95rem;">
          No participants loaded yet. Paste cells from Google Sheets above or enter manually.
        </div>
      `;
      return;
    }

    calculatePositions();
    container.innerHTML = '';

    appState.participants.forEach((p, idx) => {
      const itemEl = document.createElement('div');
      itemEl.className = 'participant-item';
      itemEl.id = `participant_row_${p.id}`;

      const posDisplay = p.position ? `${p.position}` : (idx + 1);
      const posBadgeClass = p.position === 1 ? 'p1' : (p.position === 2 ? 'p2' : (p.position === 3 ? 'p3' : ''));

      itemEl.innerHTML = `
        <div class="participant-switch-group">
          <label class="switch-label">
            <div class="switch">
              <input type="checkbox" id="toggle_${p.id}" data-id="${p.id}">
              <span class="slider"></span>
            </div>
            <span>GFX</span>
          </label>
          <span class="timer-tag" id="timer_${p.id}">10s</span>
        </div>

        <div class="participant-info">
          <div class="participant-badge-pos ${posBadgeClass}" title="${p.position ? 'Current Rank' : 'Pull Order'}">
            ${posDisplay}
          </div>
          <div class="participant-details">
            <span class="participant-vehicle">${escapeHtml(p.vehicle)}</span>
            <span class="participant-sub">${escapeHtml(p.driver)}${p.hometown ? ' • ' + escapeHtml(p.hometown) : ''}</span>
          </div>
        </div>

        <div class="participant-actions">
          <div class="distance-input-wrapper">
            <label style="font-size: 0.7rem; margin-bottom: 2px;">Distance (ft)</label>
            <input type="text" 
                   inputmode="decimal" 
                   pattern="[0-9]*\\.?[0-9]*" 
                   class="participant-distance-input" 
                   data-id="${p.id}" 
                   placeholder="000.00" 
                   value="${escapeHtml(p.distance || '')}">
          </div>

          <button type="button" class="btn btn-danger btn-sm btn-delete-participant" data-id="${p.id}" title="Remove participant">
            ✕
          </button>
        </div>

        <div class="countdown-meter-wrapper">
          <div class="countdown-progress-bar" id="meter_${p.id}"></div>
        </div>
      `;

      container.appendChild(itemEl);

      // Distance Input (mobile numeric keyboard, live rank update)
      const distInput = itemEl.querySelector('.participant-distance-input');
      distInput.addEventListener('input', (e) => {
        p.distance = e.target.value;
        Storage.save(STORAGE_KEYS.PARTICIPANTS_LIST, appState.participants);
        calculatePositions();
        updateStandingsPreview();
        updateParticipantBadges();
      });

      // Remove single participant
      const btnDelete = itemEl.querySelector('.btn-delete-participant');
      btnDelete.addEventListener('click', () => {
        appState.participants = appState.participants.filter(item => item.id !== p.id);
        Storage.save(STORAGE_KEYS.PARTICIPANTS_LIST, appState.participants);
        renderParticipantsList();
        updateStandingsPreview();
      });

      // 10s Countdown Slider for Participant
      bindCountdownSlider({
        toggleId: `toggle_${p.id}`,
        rowId: `participant_row_${p.id}`,
        meterId: `meter_${p.id}`,
        timerTagId: `timer_${p.id}`,
        durationSeconds: 10,
        getPayload: () => {
          calculatePositions();
          return {
            activeOverlay: 'participant',
            overlayData: {
              type: 'participant_lower_third',
              vehicle: p.vehicle,
              driver: p.driver,
              hometown: p.hometown,
              distance: p.distance && p.distance.trim() ? p.distance.trim() : null,
              position: p.position || null
            }
          };
        }
      });
    });
  }

  function updateParticipantBadges() {
    appState.participants.forEach((p, idx) => {
      const row = document.getElementById(`participant_row_${p.id}`);
      if (!row) return;
      const badge = row.querySelector('.participant-badge-pos');
      if (!badge) return;

      const posDisplay = p.position ? `${p.position}` : (idx + 1);
      badge.textContent = posDisplay;
      badge.className = `participant-badge-pos ${p.position === 1 ? 'p1' : (p.position === 2 ? 'p2' : (p.position === 3 ? 'p3' : ''))}`;
    });
  }

  // ==========================================================================
  // 8. Results & Standings Logic
  // ==========================================================================
  function getEffectiveClassName() {
    const classSelect = document.getElementById('classSelect');
    if (classSelect && classSelect.value === '__manual__') {
      return appState.customClass || 'Open Class';
    }
    return appState.selectedClass || 'Truck & Tractor Pulling';
  }

  function getEffectiveResultsTitle() {
    return `${appState.resultsMode} - ${getEffectiveClassName()}`;
  }

  function updateResultsTitle() {
    const titleEl = document.getElementById('resultsPreviewTitle');
    if (titleEl) {
      titleEl.textContent = getEffectiveResultsTitle();
    }
  }

  function setModeUI(mode) {
    appState.resultsMode = mode;
    Storage.save(STORAGE_KEYS.RESULTS_MODE, mode);

    const btnStandings = document.getElementById('btnModeStandings');
    const btnFinal = document.getElementById('btnModeFinal');
    if (btnStandings && btnFinal) {
      if (mode === 'Current Standings') {
        btnStandings.classList.add('active');
        btnFinal.classList.remove('active');
      } else {
        btnFinal.classList.add('active');
        btnStandings.classList.remove('active');
      }
    }
    updateResultsTitle();
  }

  function getCompiledStandings() {
    return appState.participants
      .filter(p => p.distance && !isNaN(parseFloat(p.distance)) && parseFloat(p.distance) > 0)
      .sort((a, b) => parseFloat(b.distance) - parseFloat(a.distance))
      .map((p, idx) => ({
        position: idx + 1,
        distance: p.distance,
        vehicle: p.vehicle,
        driver: p.driver,
        hometown: p.hometown
      }));
  }

  function parseBypassTSV(tsvText) {
    const lines = tsvText.split(/\r?\n/);
    const items = [];

    lines.forEach((line, idx) => {
      const trimmed = line.trim();
      if (!trimmed) return;

      const parts = line.split('\t').map(s => s.trim());
      const distance = parts[0] || '';
      const vehicle = parts[1] || '';
      const driver = parts[2] || '';
      const hometown = parts[3] || '';

      if (idx === 0 && (
        distance.toLowerCase() === 'distance' ||
        vehicle.toLowerCase() === 'vehicle' ||
        driver.toLowerCase() === 'driver'
      )) {
        return;
      }

      if (!distance && !vehicle && !driver) return;

      items.push({
        position: idx + 1,
        distance,
        vehicle,
        driver,
        hometown
      });
    });

    items.forEach((it, i) => it.position = i + 1);
    return items;
  }

  function updateStandingsPreview() {
    const tbody = document.getElementById('resultsTableBody');
    if (!tbody) return;

    const items = appState.bypassActive ? appState.bypassItems : getCompiledStandings();

    if (items.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="5" style="text-align: center; color: var(--text-muted); padding: 1.5rem;">
            ${appState.bypassActive ? 'Manual bypass is active but no valid rows found.' : 'No distance scores recorded yet. Enter distances in the Participants Panel above.'}
          </td>
        </tr>
      `;
      return;
    }

    tbody.innerHTML = items.map(it => `
      <tr>
        <td class="rank-cell">${it.position}</td>
        <td style="font-weight: 700;">${escapeHtml(it.vehicle)}</td>
        <td>${escapeHtml(it.driver)}</td>
        <td style="color: var(--text-secondary);">${escapeHtml(it.hometown || '-')}</td>
        <td class="distance-cell" style="text-align: right;">${escapeHtml(it.distance)}'</td>
      </tr>
    `).join('');
  }

  function updateOverrideBanner(isActive) {
    const banner = document.getElementById('overrideBanner');
    if (banner) {
      if (isActive) {
        banner.classList.add('active');
      } else {
        banner.classList.remove('active');
      }
    }
  }

  function broadcastResultsState() {
    const items = appState.bypassActive ? appState.bypassItems : getCompiledStandings();
    mqttService.publishState({
      activeOverlay: 'results',
      overlayData: {
        type: 'results_table',
        title: getEffectiveResultsTitle(),
        mode: appState.resultsMode,
        className: getEffectiveClassName(),
        isOverride: appState.bypassActive,
        items: items
      }
    });
  }

  // ==========================================================================
  // 9. Countdown Timers & Visual Progress Meter
  // ==========================================================================
  function bindCountdownSlider({ toggleId, rowId, meterId, timerTagId, durationSeconds, getPayload, onBeforeActivate }) {
    const toggle = document.getElementById(toggleId);
    const row = document.getElementById(rowId);
    const meter = document.getElementById(meterId);
    const timerTag = document.getElementById(timerTagId);

    if (!toggle) return;

    function startTimer() {
      cancelActiveTimer();

      toggle.checked = true;
      if (row) row.classList.add('is-active');
      if (meter) meter.style.width = '100%';
      if (timerTag) timerTag.textContent = `${durationSeconds}s`;

      const payload = getPayload();
      mqttService.publishState(payload);

      const startTime = Date.now();
      const totalMs = durationSeconds * 1000;

      const updateMeter = () => {
        const elapsed = Date.now() - startTime;
        const remainingMs = Math.max(0, totalMs - elapsed);
        const fraction = remainingMs / totalMs;

        if (meter) {
          meter.style.width = `${(fraction * 100).toFixed(1)}%`;
        }

        if (timerTag) {
          const remainingSecs = Math.ceil(remainingMs / 1000);
          timerTag.textContent = `${remainingSecs}s`;
        }

        if (remainingMs <= 0) {
          cancelActiveTimer();
        } else {
          activeTimer.animFrame = requestAnimationFrame(updateMeter);
        }
      };

      activeTimer = {
        toggle,
        row,
        meter,
        timerTag,
        animFrame: requestAnimationFrame(updateMeter),
        durationSeconds
      };
    }

    toggle.addEventListener('change', () => {
      if (toggle.checked) {
        if (typeof onBeforeActivate === 'function') {
          // Temporarily uncheck until confirmed
          toggle.checked = false;
          onBeforeActivate(
            () => startTimer(), // onConfirm
            () => { toggle.checked = false; } // onCancel
          );
        } else {
          startTimer();
        }
      } else {
        cancelActiveTimer();
      }
    });
  }

  function cancelActiveTimer() {
    if (!activeTimer) return;

    if (activeTimer.animFrame) {
      cancelAnimationFrame(activeTimer.animFrame);
    }
    if (activeTimer.toggle) {
      activeTimer.toggle.checked = false;
    }
    if (activeTimer.row) {
      activeTimer.row.classList.remove('is-active');
    }
    if (activeTimer.meter) {
      activeTimer.meter.style.width = '0%';
    }
    if (activeTimer.timerTag) {
      activeTimer.timerTag.textContent = `${activeTimer.durationSeconds || 10}s`;
    }

    activeTimer = null;

    mqttService.publishState({
      activeOverlay: 'none',
      overlayData: null
    });
  }

  // ==========================================================================
  // 10. Toast Notifications & Helpers
  // ==========================================================================
  function showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;

    container.appendChild(toast);

    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateY(10px)';
      toast.style.transition = 'all 0.3s ease';
      setTimeout(() => toast.remove(), 300);
    }, 3200);
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // Start when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
