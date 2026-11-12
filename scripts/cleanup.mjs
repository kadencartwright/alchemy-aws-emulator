import { rm } from 'node:fs/promises';
// Sessions stop their own child on scope close; never kill by name or port.
// This removes only this checkout's downloaded package, build outputs and logs.
for (const path of ['.local', 'dist']) await rm(new URL(`../${path}`, import.meta.url), { recursive: true, force: true });
console.log('Removed project bootstrap/build artifacts. Run pnpm run bootstrap before the next demo.');
