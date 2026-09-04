/**
 * Pulling Control Panel - MQTT Client Wrapper
 * Manages WebSocket MQTT connections, topic subscriptions, and retained publishing
 * using mqtt.js.
 */

export class MqttService {
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
    this.channelKey = channelKey.trim();
    this.statusCallback = onStatusChange;

    if (!this.channelKey) {
      this._updateStatus('error', 'Channel key is required');
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
      // mqtt is loaded globally from CDN
      this.client = window.mqtt.connect(this.brokerUrl, {
        clientId,
        clean: true,
        connectTimeout: 8000,
        reconnectPeriod: 3000,
        keepalive: 30
      });

      this.client.on('connect', () => {
        this.isConnected = true;
        this._updateStatus('connected', 'Connected');
        console.log(`MQTT connected to ${this.brokerUrl} with key: ${this.channelKey}`);
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
        this._updateStatus('error', err?.message || 'Connection error');
      });

      this.client.on('offline', () => {
        this.isConnected = false;
        this._updateStatus('offline', 'Offline');
      });
    } catch (err) {
      console.error('Failed to create MQTT client:', err);
      this._updateStatus('error', 'Failed to initialize MQTT');
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

  /**
   * Publish state payload to pulling-livestream/{channelKey}/state
   * with retain=true so new viewers / reloaded overlay receives it instantly.
   */
  publishState(statePayload) {
    if (!this.client || !this.isConnected) {
      console.warn('Cannot publish: MQTT client is not connected');
      return false;
    }

    const topic = `pulling-livestream/${this.channelKey}/state`;
    const message = JSON.stringify({
      ...statePayload,
      timestamp: Date.now()
    });

    this.client.publish(topic, message, { qos: 1, retain: true }, (err) => {
      if (err) {
        console.error('Failed to publish state:', err);
      } else {
        console.log(`Published state to ${topic}:`, statePayload.activeOverlay);
      }
    });
    return true;
  }

  _updateStatus(state, message) {
    if (typeof this.statusCallback === 'function') {
      this.statusCallback({ state, message });
    }
  }
}
