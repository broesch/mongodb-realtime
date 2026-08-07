/** Structural equality for JSON-ish values; identity fallback for class instances. */
export function deepEqual(a: unknown, b: unknown): boolean {
    if (Object.is(a, b)) return true;
    if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
    if (Array.isArray(a) && Array.isArray(b)) {
        return a.length === b.length && a.every((item, i) => deepEqual(item, b[i]));
    }
    if (isPlainObject(a) && isPlainObject(b)) {
        const keysA = Object.keys(a);
        const keysB = Object.keys(b);
        return keysA.length === keysB.length && keysA.every(key => key in b && deepEqual(a[key], b[key]));
    }
    return false;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
    if (typeof value !== 'object' || value === null) return false;
    const proto: unknown = Object.getPrototypeOf(value);
    return proto === Object.prototype || proto === null;
}
