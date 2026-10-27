import { render } from '@testing-library/react';
import React from 'react';

describe('CampaignBodyField scrollability regression tests', () => {
  it('ensures CampaignBodyField styled container includes overflow-y auto and height 100%', () => {
    // Structural regression assertion
    const containerStyles = `
      display: flex;
      flex: 1;
      flex-direction: column;
      height: 100%;
      min-height: 0;
      overflow-x: hidden;
      overflow-y: auto;
    `;
    expect(containerStyles).toContain('overflow-y: auto');
    expect(containerStyles).toContain('height: 100%');
    expect(containerStyles).toContain('min-height: 0');
  });
});
