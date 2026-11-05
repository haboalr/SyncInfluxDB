<?php

declare(strict_types=1);
include_once __DIR__ . '/stubs/Validator.php';
class SyncInfluxDBValidationTest extends TestCaseSymconValidation
{
    public function testValidateSyncInfluxDB(): void
    {
        $this->validateLibrary(__DIR__ . '/..');
    }
    public function testValidateSyncInfluxDBModule(): void
    {
        $this->validateModule(__DIR__ . '/../SyncInfluxDB');
    }
}
