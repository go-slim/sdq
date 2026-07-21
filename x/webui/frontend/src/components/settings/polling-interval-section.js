import { defineComponent } from "vue";

import SettingsSection from "../settings-section.js";
import { preferences, setPollingInterval } from "../../stores/preferences.js";

const intervals = [
  {value: 1000},
  {value: 2000},
  {value: 3000},
  {value: 5000},
  {value: 10000},
  {value: 15000},
  {value: 20000},
  {value: 30000},
];

export default defineComponent({
  name: "PollingIntervalSection",
  components: {SettingsSection},
  setup() {
    return {intervals, preferences, setPollingInterval};
  },
  template: `
    <SettingsSection :title="$t('settings.pollingInterval')" icon="⚡">
      <div class="py-4">
        <div class="flex justify-between items-center mb-3"><label class="text-sm font-medium text-gray-700 dark:text-[#c9d1d9]">{{ $t('settings.autoRefreshInterval') }}</label><span class="accent-text text-sm font-medium">{{ preferences.pollingInterval === 0 ? $t('common.disabled') : $formatNumber(preferences.pollingInterval / 1000) + $t('unit.secondShort') }}</span></div>
        <input type="range" min="0" max="30000" step="1000" :value="Math.min(preferences.pollingInterval, 30000)" class="accent-input w-full h-2 rounded-lg appearance-none cursor-pointer bg-gray-200 dark:bg-[#21262d]" @input="setPollingInterval(Number($event.target.value))">
        <div class="flex justify-between text-xs mt-1 text-gray-500 dark:text-[#8b949e]"><span v-for="interval in intervals" :key="interval.value">{{ $formatNumber(interval.value / 1000) }}{{ $t('unit.secondShort') }}</span></div>
        <div class="mt-4 p-3 rounded-md bg-gray-50 dark:bg-[#0d1117]"><p class="text-sm text-gray-600 dark:text-[#8b949e]">{{ preferences.pollingInterval === 0 ? $t('settings.autoRefreshDisabled') : $t('settings.autoRefreshEvery', {seconds: preferences.pollingInterval / 1000}) }}</p></div>
      </div>
    </SettingsSection>
  `,
});
