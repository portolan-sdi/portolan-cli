# Feature: ConversionStatus Enum

Defines the possible outcomes of a file conversion operation.

## Values

- [ ] SUCCESS: File was converted successfully and validated
- [ ] SKIPPED: File was already cloud-native, no conversion needed
- [ ] FAILED: Conversion threw an exception (original file preserved)
- [ ] INVALID: Conversion completed but validation failed (output kept for inspection)

## Happy Path

- [ ] ConversionStatus.SUCCESS.value returns "success"
- [ ] ConversionStatus.SKIPPED.value returns "skipped"
- [ ] ConversionStatus.FAILED.value returns "failed"
- [ ] ConversionStatus.INVALID.value returns "invalid"

## Enum Behavior

- [ ] All four status values are distinct
- [ ] Status values are strings (for JSON serialization)
- [ ] Enum members can be compared with `==`
- [ ] Enum members can be used as dictionary keys

## String Representation

- [ ] str(ConversionStatus.SUCCESS) returns "ConversionStatus.SUCCESS"
- [ ] Status values are lowercase for consistency with JSON conventions

## Invariants

- [ ] There are exactly 4 status values (no more, no less)
- [ ] All status values are non-empty strings
