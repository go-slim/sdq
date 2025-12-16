import { clsx } from 'clsx';

export function ThemePreview({ dark, color }) {
  return (
    <div className={clsx(
      "border rounded-lg overflow-hidden border-gray-300 dark:border-gray-700",
      dark ? 'bg-gray-900' : 'bg-gray-50'
    )}>
      <div className="h-80 flex flex-col items-stretch">
        <div className={clsx("flex space-x-3 p-6", dark ? 'bg-gray-800' : 'bg-white')}>
          <div className="h-3 bg-gray-300/80 rounded-full w-10"></div>
          <div className="h-3 bg-gray-300/80 rounded-full w-18"></div>
          <div className="h-3 bg-gray-300/80 rounded-full w-18"></div>
          <div className="flex-1"></div>
          <div className="h-3 bg-gray-300/80 rounded-full w-8 self-end"></div>
        </div>
        <div className="flex-1 flex flex-col space-y-6 py-6">
          <div className="flex justify-between px-12">
            <div className="h-3 bg-gray-300/80 rounded-full w-28"></div>
            <div className="flex space-x-2">
              <div className="size-3 bg-blue-500 rounded-xs"></div>
              <div className="size-3 bg-yellow-500 rounded-xs"></div>
            </div>
          </div>
          <div className="flex flex-1 items-stretch gap-4 px-12">
            <div className={clsx(
              "pt-3 flex-1 rounded-lg",
              dark ? 'bg-gray-700' : 'bg-gray-200/60'
            )}>
              <div className="py-2 px-3 bg-blue-200">
                <div className="h-3 bg-blue-400 rounded-full w-3/4"></div>
              </div>
            </div>
            <div className="space-y-1 w-28">
              <div className={clsx("w-2/3 h-3 rounded", dark ? 'bg-gray-700/90' : 'bg-gray-200/70')} />
              <div className={clsx("w-1/3 h-3 rounded", dark ? 'bg-gray-700/90' : 'bg-gray-200/70')} />
              <div className={clsx("w-4/5 h-3 rounded", dark ? 'bg-gray-700/90' : 'bg-gray-200/70')} />
              <div className={clsx("w-3/5 h-3 rounded", dark ? 'bg-gray-700/90' : 'bg-gray-200/70')} />
            </div>
          </div>
        </div>
        <div className="px-3 py-2 font-semibold text-sm border-t border-gray-300 dark:bg-gray-900 dark:border-gray-700 dark:text-white">
          {dark ? 'Dark' : 'Light'} mode with {color} accent
        </div>
      </div>
    </div>
  );
}
