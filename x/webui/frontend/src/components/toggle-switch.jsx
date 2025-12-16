import { clsx } from "clsx";
import { useEffect, useMemo, useState } from "preact/hooks";

// Toggle switch
export function ToggleSwitch({
  id,
  disabled,
  onChange,
  value,
  trueValue = true,
  falseValue = false
}) {
  const [thisValue, setValue] = useState(value ?? falseValue);

  const enabled = useMemo(
    () => thisValue === trueValue,
    [thisValue, trueValue],
  );

  const handleClick = () => {
    if (!disabled) {
      const value = enabled ? falseValue : trueValue;
      setValue(value);
      onChange?.(value);
    }
  };

  useEffect(() => {
    if (value == trueValue || value == falseValue) {
      setValue(value);
    }
  }, [value, trueValue, falseValue]);

  return (
    <button id={id}
      onClick={handleClick}
      className={clsx(
        `relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none align-middle`,
        enabled ? 'bg-blue-600' : 'bg-gray-200',
        disabled ? 'cursor-not-allowed' : 'cursor-pointer'
      )}
    >
      <span className={clsx(
        `inline-block h-4 w-4 rounded-full bg-white transition-transform`,
        enabled ? 'translate-x-6' : 'translate-x-1',
      )} />
    </button>
  );
}
