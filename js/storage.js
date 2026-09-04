/**
 * Pulling Control Panel - Local Storage Persistence Utility
 * Ensures all form values, participant lists, and states remain persistent
 * until explicitly cleared by the user.
 */

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
  RESULTS_MODE: 'pulling_results_mode', // 'standings' or 'final'
  BYPASS_RAW: 'pulling_bypass_raw',
  BYPASS_ACTIVE: 'pulling_bypass_active'
};

export const Storage = {
  save(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch (e) {
      console.warn('Storage save failed:', e);
    }
  },

  load(key, defaultValue = null) {
    try {
      const item = localStorage.getItem(key);
      return item ? JSON.parse(item) : defaultValue;
    } catch (e) {
      console.warn('Storage load failed:', e);
      return defaultValue;
    }
  },

  remove(key) {
    try {
      localStorage.removeItem(key);
    } catch (e) {
      console.warn('Storage remove failed:', e);
    }
  },

  clearAll() {
    Object.values(STORAGE_KEYS).forEach(k => {
      try {
        localStorage.removeItem(k);
      } catch (e) {}
    });
  },

  KEYS: STORAGE_KEYS
};
