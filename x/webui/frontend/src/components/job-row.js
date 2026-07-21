import { defineComponent } from "vue";

import AppLink from "./app-link.js";
import StateBadge from "./state-badge.js";
import { formatDate } from "../utils/format.js";

export default defineComponent({
  name: "JobRow",
  components: {AppLink, StateBadge},
  props: {
    job: {type: Object, required: true},
  },
  emits: ["delete", "kick"],
  setup() {
    return {formatDate};
  },
  template: `
    <tr class="border-b border-gray-200 hover:bg-gray-50 dark:border-[#30363d] dark:hover:bg-[#21262d]">
      <td class="px-6 py-4">
        <AppLink :to="'jobs/' + job.id" class="accent-text font-medium">#{{ job.id }}</AppLink>
      </td>
      <td class="px-6 py-4"><StateBadge :state="job.state" /></td>
      <td class="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{{ job.priority }}</td>
      <td class="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{{ job.reserves }}</td>
      <td class="px-6 py-4 text-sm text-gray-500 dark:text-[#8b949e]">{{ formatDate(job.created_at) }}</td>
      <td class="px-6 py-4 text-center whitespace-nowrap">
        <button v-if="job.state === 'buried'" type="button" class="bg-yellow-500 text-white px-3 py-1 rounded text-sm hover:bg-yellow-600 mr-2" @click="$emit('kick', job)">{{ $t('common.kick') }}</button>
        <button type="button" class="bg-red-600 text-white px-3 py-1 rounded text-sm hover:bg-red-700" @click="$emit('delete', job)">{{ $t('common.delete') }}</button>
      </td>
    </tr>
  `,
});
