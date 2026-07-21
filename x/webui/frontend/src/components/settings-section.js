import { defineComponent } from "vue";

export default defineComponent({
  name: "SettingsSection",
  props: {
    icon: {type: String, default: ""},
    title: {type: String, required: true},
  },
  template: `
    <section class="p-6 rounded-lg shadow-sm border mb-6 bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
      <div class="flex items-center mb-4">
        <div class="accent-icon w-8 h-8 rounded-lg flex items-center justify-center mr-3">{{ icon }}</div>
        <h2 class="text-xl font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ title }}</h2>
      </div>
      <slot />
    </section>
  `,
});
