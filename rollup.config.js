import { nodeResolve } from '@rollup/plugin-node-resolve';
import typescript from '@rollup/plugin-typescript';

export default {
  input: 'keydb-scripts-src/index.ts',
  output: {
    file: 'dist/keydb-startup.js',
    format: 'cjs'
  },
  plugins: [
    nodeResolve(),
    typescript({ module: 'ES2020' }),
  ],
};