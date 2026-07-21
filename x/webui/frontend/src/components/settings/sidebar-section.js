import { computed, defineComponent } from "vue";

import SettingsSection from "../settings-section.js";
import ToggleSwitch from "../toggle-switch.js";
import { preferences, setSidebarCollapsed } from "../../stores/preferences.js";

export default defineComponent({
  name: "SidebarSection",
  components: {SettingsSection, ToggleSwitch},
  setup() {
    const collapsed = computed({
      get: () => preferences.sidebarCollapsed,
      set: setSidebarCollapsed,
    });
    return {collapsed, preferences};
  },
  template: `
    <SettingsSection :title="$t('settings.sidebar')" icon="📱">
      <div class="py-4">
        <div class="flex items-center justify-between py-3 border-b border-gray-100 dark:border-[#21262d]"><div><div class="font-medium text-gray-900 dark:text-[#c9d1d9]">{{ $t('settings.sidebarCollapsed') }}</div><div class="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">{{ $t('settings.sidebarCollapsedDescription') }}</div></div><ToggleSwitch v-model="collapsed" /></div>
        <div class="mt-4 p-3 rounded-md bg-gray-50 dark:bg-[#0d1117]"><p class="text-sm text-gray-600 dark:text-[#8b949e]">{{ $t(preferences.sidebarCollapsed ? 'settings.sidebarCollapsedInfo' : 'settings.sidebarExpandedInfo') }}</p></div>
        <div class="mt-4 p-3 border rounded-md border-gray-200 dark:border-[#30363d]"><h4 class="text-sm font-medium mb-2 text-gray-700 dark:text-[#c9d1d9]">{{ $t('common.preview') }}</h4><div class="flex items-center space-x-2"><div :class="['accent-icon w-8 h-8 rounded transition-all', preferences.sidebarCollapsed ? 'opacity-50' : 'opacity-100']"><div class="w-full h-full flex items-center justify-center text-xs">☰</div></div><div :class="['flex-1 h-8 rounded transition-all bg-gray-100 dark:bg-[#21262d]', preferences.sidebarCollapsed ? 'opacity-30' : 'opacity-100']"></div></div></div>
      </div>
    </SettingsSection>
  `,
});
