import { defineComponent } from "vue";

export default defineComponent({
  name: "ThemePreview",
  props: {
    color: {type: String, required: true},
    dark: {type: Boolean, default: false},
  },
  template: `
    <div :style="{'--preview-accent': 'var(--color-' + color + ')'}" :class="['border rounded-lg overflow-hidden border-gray-300 dark:border-gray-700', dark ? 'bg-gray-900' : 'bg-gray-50']">
      <div class="h-80 flex flex-col items-stretch">
        <div :class="['flex space-x-3 p-6', dark ? 'bg-gray-800' : 'bg-white']"><div class="h-3 bg-gray-300/80 rounded-full w-10"></div><div class="h-3 bg-gray-300/80 rounded-full w-18"></div><div class="h-3 bg-gray-300/80 rounded-full w-18"></div><div class="flex-1"></div><div class="h-3 bg-gray-300/80 rounded-full w-8 self-end"></div></div>
        <div class="flex-1 flex flex-col space-y-6 py-6">
          <div class="flex justify-between px-12"><div class="h-3 bg-gray-300/80 rounded-full w-28"></div><div class="flex space-x-2"><div class="theme-preview-accent-dot size-3 rounded-xs"></div><div class="size-3 bg-yellow-500 rounded-xs"></div></div></div>
          <div class="flex flex-1 items-stretch gap-4 px-12">
            <div :class="['pt-3 flex-1 rounded-lg', dark ? 'bg-gray-700' : 'bg-gray-200/60']"><div class="theme-preview-accent-soft py-2 px-3"><div class="theme-preview-accent-bar h-3 rounded-full w-3/4"></div></div></div>
            <div class="space-y-1 w-28"><div v-for="width in ['w-2/3','w-1/3','w-4/5','w-3/5']" :key="width" :class="[width, 'h-3 rounded', dark ? 'bg-gray-700/90' : 'bg-gray-200/70']"></div></div>
          </div>
        </div>
        <div class="px-3 py-2 font-semibold text-sm border-t border-gray-300 dark:bg-gray-900 dark:border-gray-700 dark:text-white">{{ $t('settings.themePreview', {mode: $t(dark ? 'settings.dark' : 'settings.light'), color: $t('color.' + color)}) }}</div>
      </div>
    </div>
  `,
});
