import { i18n } from '@lingui/core';
import { I18nProvider } from '@lingui/react';
import { render, screen } from '@testing-library/react';
import { type ReactNode } from 'react';
import { FieldMetadataType } from 'twenty-shared/types';

import { EventFieldDiff } from '@/activities/timeline-activities/rows/main-object/components/EventFieldDiff';
import { type FieldMetadataItem } from '@/object-metadata/types/FieldMetadataItem';
import { type EnrichedObjectMetadataItem } from '@/object-metadata/types/EnrichedObjectMetadataItem';

jest.mock(
  '@/activities/timeline-activities/rows/main-object/components/EventFieldDiffLabel',
  () => ({
    EventFieldDiffLabel: () => <span>Field Label</span>,
  }),
);

jest.mock(
  '@/activities/timeline-activities/rows/main-object/components/EventFieldDiffValue',
  () => ({
    EventFieldDiffValue: () => <span>Field Value</span>,
  }),
);

jest.mock(
  '@/activities/timeline-activities/rows/main-object/components/EventFieldDiffValueEffect',
  () => ({
    EventFieldDiffValueEffect: () => null,
  }),
);

jest.mock(
  '@/activities/timeline-activities/rows/main-object/components/EventRelationFieldDiffValues',
  () => ({
    EventRelationFieldDiffValues: () => <span>Relation Values</span>,
  }),
);

jest.mock('@/activities/timeline-activities/utils/relationFieldChangeValue', () => ({
  isRelationFieldChangeValue: jest.fn(),
}));

const textFieldMetadataItem = {
  id: 'field-1',
  name: 'fieldName',
  type: FieldMetadataType.TEXT,
  icon: 'IconText',
  label: 'Field Label',
  options: [],
  defaultValue: undefined,
} as unknown as FieldMetadataItem;

const relationFieldMetadataItem = {
  id: 'field-2',
  name: 'relationFieldName',
  type: FieldMetadataType.RELATION,
  icon: 'IconLink',
  label: 'Relation Field Label',
  options: [],
  defaultValue: undefined,
} as unknown as FieldMetadataItem;

const mainObjectMetadataItem = {
  nameSingular: 'company',
} as unknown as EnrichedObjectMetadataItem;

const renderWithI18n = (children: ReactNode) => {
  i18n.load('en', {});
  i18n.activate('en');
  return render(<I18nProvider i18n={i18n}>{children}</I18nProvider>);
};

describe('EventFieldDiff', () => {
  it('throws an error when fieldMetadataItem is undefined', () => {
    expect(() =>
      renderWithI18n(
        <EventFieldDiff
          fieldDiff={{ before: 'old', after: 'new' }}
          mainObjectMetadataItem={mainObjectMetadataItem}
          fieldMetadataItem={undefined}
          diffArtificialRecordStoreId="diff-id"
        />,
      ),
    ).toThrow('fieldMetadataItem is required');
  });

  it('renders the field value and tooltip title when field is updated to a non-empty string', () => {
    const { container } = renderWithI18n(
      <EventFieldDiff
        fieldDiff={{ before: 'old value', after: 'a very long description string that overflows' }}
        mainObjectMetadataItem={mainObjectMetadataItem}
        fieldMetadataItem={textFieldMetadataItem}
        diffArtificialRecordStoreId="diff-id"
      />,
    );

    expect(screen.getByText('Field Value')).toBeInTheDocument();
    expect(container.firstChild).toHaveAttribute('title');
  });
});
