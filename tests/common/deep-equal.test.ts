import { deepEqual } from '../../src/common/deep-equal.js';

describe('deepEqual', () => {
    it.each([
        [1, 1, true],
        ['a', 'b', false],
        [null, null, true],
        [null, undefined, false],
        [[1, 2], [1, 2], true],
        [[1, 2], [2, 1], false],
        [{ a: 1, b: [2] }, { b: [2], a: 1 }, true],
        [{ a: 1 }, { a: 1, b: 2 }, false],
        [new Date(0), new Date(0), true],
        [new Date(0), new Date(1), false],
    ])('deepEqual(%j, %j) === %s', (a, b, expected) => {
        expect(deepEqual(a, b)).toBe(expected);
    });

    it('falls back to identity for class instances', () => {
        class X {
            constructor(public v: number) {}
        }
        const x = new X(1);
        expect(deepEqual(x, x)).toBe(true);
        expect(deepEqual(new X(1), new X(1))).toBe(false);
    });
});
