import { computed, defineComponent } from "vue";

import { hrefFor, navigate } from "../router.js";

export default defineComponent({
  name: "AppLink",
  props: {
    to: {type: String, default: ""},
  },
  setup(props) {
    const href = computed(() => hrefFor(props.to));
    function open(event) {
      if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
        return;
      }
      event.preventDefault();
      navigate(props.to);
    }
    return {href, open};
  },
  template: `<a :href="href" @click="open"><slot /></a>`,
});
