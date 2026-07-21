import { computed, defineComponent, ref } from "vue";
import Moon from "@lucide/vue/icons/moon.mjs";
import Sun from "@lucide/vue/icons/sun.mjs";

import { t } from "../../i18n.js";
import ToggleSwitch from "../toggle-switch.js";
import ThemePreview from "./theme-preview.js";
import {
  isDark,
  preferences,
  setColor,
  setTheme,
  supportedColors,
} from "../../stores/preferences.js";

const ColorButton = defineComponent({
  name: "ColorButton",
  props: {
    active: {type: Boolean, default: false},
    value: {type: String, required: true},
  },
  emits: ["hover", "select"],
  template: `
    <button
      type="button"
      :aria-label="$t('settings.useAccent', {color: $t('color.' + value)})"
      :aria-pressed="active"
      :style="{'--color-current': 'var(--color-' + value + ')'}"
      :class="[
        'ring-0 p-0 size-8 rounded-full cursor-pointer transition-all ease-in-out hover:ring-2 hover:p-1',
        active ? 'ring-2 p-1 ring-[var(--color-current)]' : 'ring-gray-200 hover:ring-gray-400',
      ]"
      @click="$emit('select', value)"
      @mouseenter="$emit('hover', value)"
      @mouseleave="$emit('hover', null)"
    ><span class="block rounded-full bg-[var(--color-current)] size-full"></span></button>
  `,
});

export default defineComponent({
  name: "ThemePreviewWrapper",
  components: {ColorButton, Moon, Sun, ThemePreview, ToggleSwitch},
  props: {
    dark: {type: Boolean, default: false},
  },
  setup(props) {
    const hoveringColor = ref(null);
    const active = computed(() => props.dark === isDark.value);
    const modeKey = computed(() => props.dark ? "dark" : "light");
    const selectedColor = computed(() => preferences[`${modeKey.value}Color`]);
    const previewColor = computed(() => hoveringColor.value || selectedColor.value);
    const mode = computed(() => t(props.dark ? "settings.dark" : "settings.light"));
    const alternateTheme = computed(() => props.dark ? "light" : "dark");
    const selectedTheme = computed({
      get: () => preferences.theme,
      set: setTheme,
    });
    const selectColor = (value) => setColor(modeKey.value, value);
    return {
      active,
      alternateTheme,
      hoveringColor,
      mode,
      preferences,
      previewColor,
      selectedColor,
      selectedTheme,
      selectColor,
      supportedColors,
    };
  },
  template: `
    <div
      :style="{'--theme-accent': 'var(--color-' + selectedColor + ')'}"
      :class="['theme-mode-card border rounded-md overflow-hidden mb-12', active ? 'is-active' : 'border-gray-300 dark:border-gray-700']"
    >
      <div :class="['theme-mode-header px-6 py-4 border-b', active ? '' : 'border-gray-300 dark:border-gray-700 dark:bg-gray-800']">
        <div class="flex items-center gap-2 h-6.5"><Moon v-if="dark" :size="18" /><Sun v-else :size="18" /><span class="font-semibold">{{ $t(dark ? 'settings.themeDark' : 'settings.themeLight') }}</span><span class="flex-1"></span><span v-if="active" class="theme-mode-active text-xs font-semibold px-2 py-1 rounded-full border">{{ $t('common.active') }}</span></div>
      </div>
      <div class="px-6 my-4"><p class="text-[#8b949e] text-sm">{{ $t('settings.themeActiveWhen', {mode: mode.toLocaleLowerCase()}) }}</p></div>
      <div :class="['px-6 pb-6', dark ? 'dark' : 'light']"><ThemePreview :dark="dark" :color="previewColor" /></div>
      <div class="flex items-center justify-between gap-2 mx-6 mb-4">
        <div class="flex gap-2"><ColorButton v-for="color in supportedColors" :key="color" :value="color" :active="selectedColor === color" @hover="hoveringColor = $event" @select="selectColor" /></div>
        <ToggleSwitch v-if="preferences.theme !== 'system'" v-model="selectedTheme" :true-value="dark ? 'dark' : 'light'" :false-value="alternateTheme" />
      </div>
    </div>
  `,
});
