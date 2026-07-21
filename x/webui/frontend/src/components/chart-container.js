import { computed, defineComponent, ref } from "vue";

import { formatNumber, t } from "../i18n.js";

const chartFrame = {
  bottom: 42,
  height: 250,
  left: 54,
  right: 18,
  top: 18,
  width: 640,
};

const barColors = ["#2563eb", "#d97706", "#7c3aed", "#dc2626", "#059669"];
let chartSequence = 0;

function clamp(value, minimum, maximum) {
  return Math.min(Math.max(value, minimum), maximum);
}

function formatValue(value) {
  return formatNumber(value, {
    maximumFractionDigits: Number.isInteger(value) ? 0 : 2,
  });
}

function niceUpperBound(value) {
  if (!Number.isFinite(value) || value <= 0) {
    return 1;
  }

  const roughStep = value / 4;
  const magnitude = 10 ** Math.floor(Math.log10(roughStep));
  const normalized = roughStep / magnitude;
  const factor = normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
  return factor * magnitude * 4;
}

function normalizeData(data) {
  return data.flatMap((item, index) => {
    const value = Number(item?.value);
    if (!Number.isFinite(value)) {
      return [];
    }

    const label = String(item.time ?? item.name ?? index + 1);
    return [{label, shortLabel: label.length > 12 ? `${label.slice(0, 11)}...` : label, value}];
  });
}

export default defineComponent({
  name: "ChartContainer",
  props: {
    data: {type: Array, default: () => []},
    title: {type: String, required: true},
    type: {
      type: String,
      default: "line",
      validator: (value) => value === "line" || value === "bar",
    },
  },
  setup(props) {
    const activeIndex = ref(-1);
    const chartID = `sdq-chart-${++chartSequence}`;
    const gradientID = `${chartID}-fill`;
    const plotWidth = chartFrame.width - chartFrame.left - chartFrame.right;
    const plotHeight = chartFrame.height - chartFrame.top - chartFrame.bottom;

    const items = computed(() => normalizeData(props.data));
    const upperBound = computed(() => niceUpperBound(Math.max(0, ...items.value.map((item) => item.value))));
    const yTicks = computed(() => Array.from({length: 5}, (_, index) => ({
      label: formatValue(upperBound.value * (4 - index) / 4),
      value: upperBound.value * (4 - index) / 4,
      y: chartFrame.top + plotHeight * index / 4,
    })));
    const points = computed(() => items.value.map((item, index) => {
      const x = items.value.length === 1
        ? chartFrame.left + plotWidth / 2
        : chartFrame.left + plotWidth * index / (items.value.length - 1);
      const y = chartFrame.top + plotHeight * (1 - item.value / upperBound.value);
      return {...item, x, y};
    }));
    const bars = computed(() => {
      const step = plotWidth / Math.max(items.value.length, 1);
      const width = Math.min(72, step * 0.58);
      return items.value.map((item, index) => {
        const height = plotHeight * item.value / upperBound.value;
        return {
          ...item,
          color: barColors[index % barColors.length],
          height,
          width,
          x: chartFrame.left + step * index + (step - width) / 2,
          y: chartFrame.top + plotHeight - height,
        };
      });
    });
    const linePath = computed(() => points.value
      .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
      .join(" "));
    const areaPath = computed(() => {
      if (points.value.length === 0) {
        return "";
      }
      const baseline = chartFrame.top + plotHeight;
      return `${linePath.value} L ${points.value.at(-1).x} ${baseline} L ${points.value[0].x} ${baseline} Z`;
    });
    const activeItem = computed(() => {
      const source = props.type === "line" ? points.value : bars.value;
      return source[activeIndex.value] || null;
    });
    const tooltipStyle = computed(() => {
      if (!activeItem.value) {
        return {};
      }
      const x = props.type === "bar"
        ? activeItem.value.x + activeItem.value.width / 2
        : activeItem.value.x;
      return {
        left: `${clamp(x / chartFrame.width * 100, 13, 87)}%`,
        top: `${clamp(activeItem.value.y / chartFrame.height * 100, 24, 88)}%`,
      };
    });
    const summary = computed(() => {
      if (items.value.length === 0) {
        return t("chart.noData");
      }
      if (props.type === "line") {
        return t("chart.latest", {value: formatValue(items.value.at(-1).value)});
      }
      const total = items.value.reduce((sum, item) => sum + item.value, 0);
      return t("chart.total", {value: formatValue(total)});
    });
    const accessibleLabel = computed(() => `${props.title}. ${items.value
      .map((item) => `${item.label}: ${formatValue(item.value)}`)
      .join(", ")}`);

    function clearActive() {
      activeIndex.value = -1;
    }

    function setActive(index) {
      activeIndex.value = index;
    }

    function showXAxisLabel(index) {
      if (items.value.length <= 7) {
        return true;
      }
      const interval = Math.ceil(items.value.length / 6);
      return index === items.value.length - 1 || index % interval === 0;
    }

    return {
      accessibleLabel,
      activeIndex,
      activeItem,
      areaPath,
      bars,
      chartFrame,
      clearActive,
      formatValue,
      gradientID,
      items,
      linePath,
      points,
      setActive,
      showXAxisLabel,
      summary,
      tooltipStyle,
      yTicks,
    };
  },
  template: `
    <article class="sdq-chart-card p-6 rounded-lg shadow-sm border bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
      <header class="sdq-chart-header">
        <h3 class="text-lg font-semibold text-gray-900 dark:text-[#c9d1d9]">{{ title }}</h3>
        <span class="sdq-chart-summary">{{ summary }}</span>
      </header>

      <div v-if="items.length === 0" class="sdq-chart-empty">{{ $t('chart.noDataAvailable') }}</div>
      <div v-else class="sdq-chart-stage" @mouseleave="clearActive">
        <svg
          class="sdq-chart-svg"
          :viewBox="'0 0 ' + chartFrame.width + ' ' + chartFrame.height"
          preserveAspectRatio="none"
          role="img"
          :aria-label="accessibleLabel"
        >
          <defs>
            <linearGradient :id="gradientID" x1="0" y1="0" x2="0" y2="1">
              <stop class="sdq-chart-area-start" offset="0%" stop-opacity="0.24" />
              <stop class="sdq-chart-area-end" offset="100%" stop-opacity="0.01" />
            </linearGradient>
          </defs>

          <g v-for="tick in yTicks" :key="tick.value">
            <line
              class="sdq-chart-grid"
              :x1="chartFrame.left"
              :x2="chartFrame.width - chartFrame.right"
              :y1="tick.y"
              :y2="tick.y"
            />
            <text class="sdq-chart-axis-label" :x="chartFrame.left - 10" :y="tick.y + 4" text-anchor="end">{{ tick.label }}</text>
          </g>
          <line
            class="sdq-chart-axis"
            :x1="chartFrame.left"
            :x2="chartFrame.width - chartFrame.right"
            :y1="chartFrame.height - chartFrame.bottom"
            :y2="chartFrame.height - chartFrame.bottom"
          />

          <template v-if="type === 'line'">
            <path class="sdq-chart-area" :d="areaPath" :fill="'url(#' + gradientID + ')'" />
            <path class="sdq-chart-line" :d="linePath" />
            <g
              v-for="(point, index) in points"
              :key="point.label + index"
              class="sdq-chart-point-group"
              tabindex="0"
              @mouseenter="setActive(index)"
              @focus="setActive(index)"
              @blur="clearActive"
            >
              <title>{{ point.label }}: {{ formatValue(point.value) }}</title>
              <line
                v-if="activeIndex === index"
                class="sdq-chart-guide"
                :x1="point.x"
                :x2="point.x"
                :y1="chartFrame.top"
                :y2="chartFrame.height - chartFrame.bottom"
              />
              <circle class="sdq-chart-target" :cx="point.x" :cy="point.y" r="15" />
              <circle v-if="activeIndex === index" class="sdq-chart-point-halo" :cx="point.x" :cy="point.y" r="8" />
              <circle class="sdq-chart-point" :cx="point.x" :cy="point.y" :r="activeIndex === index ? 5 : 4" />
              <text
                v-if="showXAxisLabel(index)"
                class="sdq-chart-axis-label"
                :x="point.x"
                :y="chartFrame.height - 16"
                text-anchor="middle"
              >{{ point.shortLabel }}</text>
            </g>
          </template>

          <template v-else>
            <g
              v-for="(bar, index) in bars"
              :key="bar.label + index"
              class="sdq-chart-bar-group"
              tabindex="0"
              @mouseenter="setActive(index)"
              @focus="setActive(index)"
              @blur="clearActive"
            >
              <title>{{ bar.label }}: {{ formatValue(bar.value) }}</title>
              <rect
                v-if="activeIndex === index"
                class="sdq-chart-bar-focus"
                :x="bar.x - 7"
                :y="chartFrame.top"
                :width="bar.width + 14"
                :height="chartFrame.height - chartFrame.bottom - chartFrame.top"
                rx="7"
              />
              <rect
                class="sdq-chart-bar"
                :x="bar.x"
                :y="bar.y"
                :width="bar.width"
                :height="bar.height"
                :fill="bar.color"
                rx="5"
              />
              <text class="sdq-chart-value" :x="bar.x + bar.width / 2" :y="Math.max(bar.y - 8, chartFrame.top + 12)" text-anchor="middle">{{ formatValue(bar.value) }}</text>
              <text class="sdq-chart-axis-label" :x="bar.x + bar.width / 2" :y="chartFrame.height - 16" text-anchor="middle">{{ bar.shortLabel }}</text>
            </g>
          </template>
        </svg>

        <div v-if="activeItem" class="sdq-chart-tooltip" :style="tooltipStyle" aria-live="polite">
          <span>{{ activeItem.label }}</span>
          <strong>{{ formatValue(activeItem.value) }}</strong>
        </div>
      </div>
    </article>
  `,
});
