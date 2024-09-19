export enum FieldType {
  Boolean = 'boolean',
  Readonly = 'readonly',
  Text = 'text',
  Enum = 'enum',
  Integer = 'integer',
  Hidden = 'hidden', // Needed here or handled in other way?
  BooleanOrAuto = 'booleanOrAuto(-1)',
  IntegerOrAuto = 'integerOrAuto(-1)',
  BooleanOrDefaultFromConfig = 'booleanOrDefaultFromConfig(-1)'
}
