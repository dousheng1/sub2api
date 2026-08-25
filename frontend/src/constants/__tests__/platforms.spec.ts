import { describe, expect, it } from 'vitest'
import {
  ACCOUNT_PLATFORM_OPTIONS,
  CONCRETE_PLATFORM_OPTIONS,
  GROUP_PLATFORM_OPTIONS
} from '@/constants/platforms'

const concretePlatforms = [
  'anthropic',
  'openai',
  'gemini',
  'antigravity',
  'grok',
  'kimi',
  'zhipu',
  'deepseek'
]

describe('platform option catalogs', () => {
  it('exposes every concrete request platform', () => {
    expect(CONCRETE_PLATFORM_OPTIONS.map((option) => option.value)).toEqual(concretePlatforms)
  })

  it('adds non-LLM providers for account filters', () => {
    expect(ACCOUNT_PLATFORM_OPTIONS.map((option) => option.value)).toEqual([
      ...concretePlatforms,
      'serper'
    ])
  })

  it('adds composite for group-backed filters', () => {
    expect(GROUP_PLATFORM_OPTIONS.map((option) => option.value)).toEqual([
      ...concretePlatforms,
      'serper',
      'composite'
    ])
  })
})
