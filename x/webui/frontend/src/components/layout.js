import { computed, defineComponent } from "vue";
import LineChartIcon from "@lucide/vue/icons/chart-line.mjs";
import DatabaseIcon from "@lucide/vue/icons/database.mjs";
import LanguagesIcon from "@lucide/vue/icons/languages.mjs";
import ListTodoIcon from "@lucide/vue/icons/list-todo.mjs";
import MenuIcon from "@lucide/vue/icons/menu.mjs";
import MonitorIcon from "@lucide/vue/icons/monitor.mjs";
import MoonIcon from "@lucide/vue/icons/moon.mjs";
import SettingsIcon from "@lucide/vue/icons/settings.mjs";
import SunIcon from "@lucide/vue/icons/sun.mjs";

import { route } from "../router.js";
import {
  activeLanguage,
  isDark,
  preferences,
  setLanguage,
  setSidebarCollapsed,
  toggleTheme,
} from "../stores/preferences.js";
import AppLink from "./app-link.js";

export default defineComponent({
  name: "AppLayout",
  components: {AppLink, LanguagesIcon, MenuIcon},
  setup() {
    const themeIcon = computed(() => preferences.theme === "system"
      ? MonitorIcon
      : preferences.theme === "dark" ? MoonIcon : SunIcon);
    const navigation = [
      {labelKey: "nav.topics", to: "", icon: ListTodoIcon, routes: ["dashboard", "topics", "topic", "job"]},
      {labelKey: "nav.storage", to: "storage", icon: DatabaseIcon, routes: ["storage"]},
      {labelKey: "nav.metrics", to: "metrics", icon: LineChartIcon, routes: ["metrics"]},
    ];
    const settings = {labelKey: "nav.settings", to: "settings", icon: SettingsIcon, routes: ["settings"]};
    const isActive = (item) => item.routes.includes(route.name);
    const toggleLanguage = () => setLanguage(activeLanguage.value === "zh" ? "en" : "zh");
    return {
      activeLanguage,
      isActive,
      isDark,
      navigation,
      preferences,
      setSidebarCollapsed,
      settings,
      themeIcon,
      toggleLanguage,
      toggleTheme,
    };
  },
  template: `
    <header class="sticky top-0 z-20 h-0">
      <div :class="['flex items-center shadow-sm h-16 gap-2 border-b', isDark ? 'bg-[#161b22] border-[#30363d]' : 'bg-white border-gray-200']">
        <div class="shrink-0 size-16 flex items-center justify-center">
          <button
            type="button"
            :title="$t('layout.toggleSidebar')"
            :aria-label="$t('layout.toggleSidebar')"
            :class="['size-12 content-center cursor-pointer transition-colors rounded-lg', isDark ? 'text-[#c9d1d9] hover:bg-[#21262d]' : 'text-gray-700 hover:bg-gray-100']"
            @click="setSidebarCollapsed(!preferences.sidebarCollapsed)"
          ><MenuIcon class="mx-auto" /></button>
        </div>
        <div class="flex-1">
          <h1 :class="['text-2xl font-bold', isDark ? 'text-[#c9d1d9]' : 'text-gray-900']">{{ $t('app.title') }}</h1>
        </div>
        <div class="shrink-0 size-16 flex items-center justify-center">
          <button
            type="button"
            :title="$t('layout.changeLanguage')"
            :aria-label="$t('layout.changeLanguage')"
            :class="['size-12 content-center cursor-pointer transition-colors rounded-lg', isDark ? 'text-[#c9d1d9] hover:bg-[#21262d]' : 'text-gray-700 hover:bg-gray-100']"
            @click="toggleLanguage"
          ><LanguagesIcon :class="['language-icon mx-auto', activeLanguage === 'zh' ? 'is-zh' : 'is-en']" /></button>
        </div>
        <div class="shrink-0 size-16 flex items-center justify-center">
          <button
            type="button"
            :title="$t('layout.changeTheme')"
            :aria-label="$t('layout.changeTheme')"
            :class="['size-12 content-center cursor-pointer transition-colors rounded-lg', isDark ? 'text-[#c9d1d9] hover:bg-[#21262d]' : 'text-gray-700 hover:bg-gray-100']"
            @click="toggleTheme"
          ><component :is="themeIcon" class="mx-auto" /></button>
        </div>
      </div>
    </header>
    <div :class="['flex flex-1 min-h-screen', isDark ? 'bg-[#0d1117]' : 'bg-gray-50']">
      <aside :class="['flex flex-col justify-between h-screen sticky top-0 z-10 pt-20 pb-4 transition-all overflow-hidden', preferences.sidebarCollapsed ? 'w-16' : 'w-64']">
        <nav>
          <AppLink
            v-for="item in navigation"
            :key="item.to"
            :to="item.to"
            :aria-label="$t(item.labelKey)"
            :title="preferences.sidebarCollapsed ? $t(item.labelKey) : undefined"
            :class="[
              'flex items-center h-12 rounded-r-full capitalize transition-colors cursor-pointer',
              isActive(item)
                ? 'accent-surface'
                : isDark ? 'text-[#c9d1d9] hover:bg-[#21262d]' : 'text-gray-700 hover:bg-gray-100'
            ]"
          >
            <span class="shrink-0 w-16 flex items-center justify-center"><component :is="item.icon" /></span>
            <span v-if="!preferences.sidebarCollapsed" class="min-w-0 flex-1 whitespace-nowrap overflow-hidden pointer-events-none">{{ $t(item.labelKey) }}</span>
          </AppLink>
        </nav>
        <AppLink
          :to="settings.to"
          :aria-label="$t(settings.labelKey)"
          :title="preferences.sidebarCollapsed ? $t(settings.labelKey) : undefined"
          :class="[
            'flex items-center h-12 rounded-r-full capitalize transition-colors cursor-pointer',
            isActive(settings)
              ? 'accent-surface'
              : isDark ? 'text-[#c9d1d9] hover:bg-[#21262d]' : 'text-gray-700 hover:bg-gray-100'
          ]"
        >
          <span class="shrink-0 w-16 flex items-center justify-center"><component :is="settings.icon" /></span>
          <span v-if="!preferences.sidebarCollapsed" class="min-w-0 flex-1 whitespace-nowrap overflow-hidden pointer-events-none">{{ $t(settings.labelKey) }}</span>
        </AppLink>
      </aside>
      <main class="flex-1 min-w-0 pt-24 pb-8 px-4 sm:px-6 lg:px-8">
        <div data-page-shell class="w-full max-w-7xl mx-auto"><slot /></div>
      </main>
    </div>
  `,
});
