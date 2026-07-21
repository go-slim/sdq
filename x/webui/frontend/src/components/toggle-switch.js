import { computed, defineComponent } from "vue";

export default defineComponent({
  name: "ToggleSwitch",
  props: {
    disabled: {type: Boolean, default: false},
    falseValue: {default: false},
    modelValue: {default: false},
    trueValue: {default: true},
  },
  emits: ["update:modelValue"],
  setup(props, {emit}) {
    const enabled = computed(() => props.modelValue === props.trueValue);
    function toggle() {
      if (!props.disabled) emit("update:modelValue", enabled.value ? props.falseValue : props.trueValue);
    }
    return {enabled, toggle};
  },
  template: `
    <button
      type="button"
      role="switch"
      :aria-checked="enabled"
      :disabled="disabled"
      :class="[
        'relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none align-middle',
        enabled ? 'accent-solid' : 'bg-gray-200 dark:bg-[#21262d]',
        disabled ? 'cursor-not-allowed' : 'cursor-pointer'
      ]"
      @click="toggle"
    >
      <span :class="['inline-block h-4 w-4 rounded-full bg-white transition-transform', enabled ? 'translate-x-6' : 'translate-x-1']"></span>
    </button>
  `,
});
