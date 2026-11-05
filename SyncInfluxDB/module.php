<?php

declare(strict_types=1);

/**
 * Sync InfluxDB
 *
 * InfluxDB counterpart of symcon/SyncMySQL. Periodically transfers the raw archive
 * values of selected variables to InfluxDB (1.x, 2.x/Cloud, 3 Core/Enterprise) as
 * line protocol. Metadata (identification, unit, meter, zone, tariff, ...) is written
 * as tags instead of relational tables.
 *
 * Delivery is at-least-once: the sync position per variable only advances after
 * InfluxDB acknowledged the write. Re-sent points carry the same series key and
 * timestamp, so InfluxDB overwrites them and no duplicates arise.
 */
class SyncInfluxDB extends IPSModule
{
    private const ARCHIVE_GUID = '{43192F0B-135B-4CE7-A0A7-1475603F3060}';

    // AC_GetLoggedValues returns at most this many rows per call
    private const AC_LIMIT = 10000;

    // Adaptive read window for archive paging (seconds)
    private const WINDOW_INITIAL = 86400;
    private const WINDOW_MAX = 10 * 365 * 86400;

    // Do not read the last seconds, archive control may still be buffering them
    private const ARCHIVE_LAG = 10;

    private const STATUS_CONNECTION_ERROR = 200;
    private const STATUS_CONFIG_ERROR = 201;

    // BKS identification as used by SyncMySQL (Hochschule Biberach / Smart Energy Box)
    // https://regex101.com/r/vP6TqD/3
    private $identFormats = [
        '', // Custom
        "^([A-Za-z0-9]+)-([_A-Za-z0-9]+)-([_A-Za-z0-9]+)-([_A-Za-z0-9]+)=([_0-9]+)\+([_A-Za-z0-9]+)-([_A-Za-z0-9]+)-([_0-9]+)([_A-Z])([_0-9]+)\/([A-Za-z0-9]+)-([_A-Za-z0-9]+)-([_A-Za-z0-9]+)-([_A-Za-z0-9]*)-([_A-Za-z0-9]*)-([_A-Za-z0-9]*)-([_A-Za-z0-9]*)-([_A-Za-z0-9]*)$"
    ];

    // https://regex101.com/r/peWokG/4
    private $bksFormats = [
        '', // Custom
        "^([A-Za-z0-9]+)-([_A-Za-z0-9]+)-([_A-Za-z0-9]+)-([_A-Za-z0-9]+)=([_0-9]+)\+([_A-Za-z0-9]+)-([_A-Za-z0-9]+)-([_0-9]+)([_A-Z])([_0-9]+)$"
    ];

    // Regex group -> tag key (same order as SyncMySQL's format table)
    private $formatTagKeys = [
        1  => 'property',
        2  => 'property_detail',
        3  => 'ifp_building_designation',
        4  => 'ifp_room_designation',
        5  => 'system_number',
        6  => 'component_building_designation',
        7  => 'component_room_designation',
        8  => 'circuit_diagram_page',
        9  => 'classification_scheme',
        10 => 'circuit_diagram_number',
        11 => 'function_identifier',
        12 => 'data_point',
        13 => 'additional_field_01',
        14 => 'additional_field_02',
        15 => 'additional_field_03',
        16 => 'additional_field_04',
        17 => 'additional_field_05',
        18 => 'additional_field_06'
    ];

    public function Create()
    {
        //Never delete this line!
        parent::Create();

        // Connection
        $this->RegisterPropertyBoolean('Active', false);
        $this->RegisterPropertyInteger('APIVersion', 2);
        $this->RegisterPropertyString('URL', 'http://localhost:8086');
        $this->RegisterPropertyBoolean('SSLDontVerify', false);
        $this->RegisterPropertyString('Organization', '');
        $this->RegisterPropertyString('Bucket', 'symcon');
        $this->RegisterPropertyString('Token', '');
        $this->RegisterPropertyString('Username', '');
        $this->RegisterPropertyString('Password', '');
        $this->RegisterPropertyInteger('Interval', 300);

        // Write options
        $this->RegisterPropertyString('Measurement', 'symcon');
        $this->RegisterPropertyInteger('InitialSync', 0);
        $this->RegisterPropertyInteger('Format', 0);
        $this->RegisterPropertyBoolean('FormatTags', true);
        $this->RegisterPropertyInteger('BatchSize', 5000);
        $this->RegisterPropertyInteger('MaxValuesPerRun', 200000);
        $this->RegisterPropertyBoolean('Compress', true);
        $this->RegisterPropertyBoolean('AutoEnableLogging', true);

        // Metadata
        $this->RegisterPropertyString('Meters', '[]');
        $this->RegisterPropertyString('Zones', '[]');
        $this->RegisterPropertyString('Variables', '[]');

        // VariableID => timestamp of the last transferred value (inclusive)
        $this->RegisterAttributeString('SyncState', '{}');

        $this->RegisterTimer('Sync', 0, "SINF_Sync(\$_IPS['TARGET']);");
    }

    public function ApplyChanges()
    {
        //Never delete this line!
        parent::ApplyChanges();

        $this->SetBuffer('ConnectionStatus', '');

        if (!$this->ReadPropertyBoolean('Active')) {
            $this->SetTimerInterval('Sync', 0);
            $this->SetStatus(IS_INACTIVE);
        } elseif (($error = $this->validateConfiguration()) !== '') {
            $this->SetBuffer('ConnectionStatus', $error);
            $this->SetTimerInterval('Sync', 0);
            $this->SetStatus(self::STATUS_CONFIG_ERROR);
        } else {
            $this->SetTimerInterval('Sync', $this->ReadPropertyInteger('Interval') * 1000);
            $this->SetStatus(IS_ACTIVE);
        }

        $this->pruneSyncState();
        $this->updateStatusLabel();

        // Links and archive settings touch other objects -> wait for the kernel
        if (IPS_GetKernelRunlevel() == KR_READY) {
            $this->UpdateLinks();
            $this->UpdateArchive();
        } else {
            $this->RegisterMessage(0, IPS_KERNELSTARTED);
        }
    }

    public function MessageSink($TimeStamp, $SenderID, $Message, $Data)
    {
        if ($Message == IPS_KERNELSTARTED) {
            $this->UnregisterMessage(0, IPS_KERNELSTARTED);
            $this->UpdateLinks();
            $this->UpdateArchive();
        }
    }

    public function RequestAction($Ident, $Value)
    {
        switch ($Ident) {
            case 'APIVersion':
                $this->updateVersionFields((int) $Value);
                break;
            case 'Format':
                $this->UpdateFormField('FormatTags', 'visible', ((int) $Value) > 0);
                break;
            case 'BulkAdd':
                $data = json_decode($Value, true);
                echo $this->bulkAddVariables(
                    intval($data['Parent'] ?? 0),
                    (bool) ($data['Recursive'] ?? true),
                    (bool) ($data['OnlyLogged'] ?? false),
                    intval($data['AggregationType'] ?? 0),
                    (string) ($data['Unit'] ?? '')
                );
                break;
            case 'ResetFromForm':
                $data = json_decode($Value, true);
                $from = json_decode($data['From'] ?? '', true);
                $timestamp = 0;
                if (is_array($from) && ($from['year'] ?? 0) > 0) {
                    $timestamp = mktime($from['hour'] ?? 0, $from['minute'] ?? 0, $from['second'] ?? 0, $from['month'], $from['day'], $from['year']);
                }
                $this->ResetSyncState((int) ($data['VariableID'] ?? 0), $timestamp);
                echo $this->Translate('Sync state reset');
                break;
            default:
                throw new Exception('Invalid Ident');
        }
    }

    public function GetConfigurationForm()
    {
        $form = json_decode(file_get_contents(__DIR__ . '/form.json'), true);
        $apiVersion = $this->ReadPropertyInteger('APIVersion');
        $format = $this->ReadPropertyInteger('Format');

        // Error label
        $error = $this->GetBuffer('ConnectionStatus');
        $this->patchElement($form['elements'], 'ConnectionStatus', [
            'visible' => $error !== '',
            'caption' => $this->Translate('Error') . ': ' . $error
        ]);

        // Version dependent fields
        foreach ($this->versionFieldVisibility($apiVersion) as $name => $visible) {
            $this->patchElement($form['elements'], $name, ['visible' => $visible]);
        }
        $this->patchElement($form['elements'], 'FormatTags', ['visible' => $format > 0]);
        $this->patchElement($form['elements'], 'TestButton', ['enabled' => $this->ReadPropertyBoolean('Active')]);
        $this->patchElement($form['elements'], 'SyncButton', ['enabled' => $this->ReadPropertyBoolean('Active')]);

        // Variable list: validation, meter/zone options and computed columns
        $variables = &$this->findElement($form['elements'], 'Variables');
        foreach ($variables['columns'] as &$column) {
            switch ($column['name']) {
                case 'Ident':
                    $column['edit']['validate'] = $this->identFormats[$format];
                    break;
                case 'Meter':
                    $column['edit']['options'] = $this->getMeterOptions();
                    break;
                case 'Zone':
                    $column['edit']['options'] = $this->getZoneOptions();
                    break;
            }
        }
        unset($column);
        $variables['values'] = $this->getVariableListValues();
        unset($variables);

        // BKS validation for meters
        $meters = &$this->findElement($form['elements'], 'Meters');
        foreach ($meters['columns'] as &$column) {
            if ($column['name'] == 'BKS') {
                $column['edit']['validate'] = $this->bksFormats[$format];
            }
        }
        unset($column, $meters);

        return json_encode($form);
    }

    /**
     * Transfer all new archive values of the active variables to InfluxDB.
     */
    public function Sync()
    {
        if (!$this->ReadPropertyBoolean('Active')) {
            echo $this->Translate('Instance is inactive');
            return;
        }
        if (!IPS_SemaphoreEnter('SINF_Sync' . $this->InstanceID, 100)) {
            echo $this->Translate('Aborting. Sync is still in progress...');
            return;
        }

        try {
            $archiveID = IPS_GetInstanceListByModuleID(self::ARCHIVE_GUID)[0];
            $state = $this->readSyncState();
            $budget = $this->ReadPropertyInteger('MaxValuesPerRun');
            $batchSize = $this->ReadPropertyInteger('BatchSize');
            $initialSync = $this->ReadPropertyInteger('InitialSync');
            $now = time() - self::ARCHIVE_LAG;
            $total = 0;

            $rows = $this->getActiveRows();
            $this->SendDebug('Sync', sprintf('Starting... Checking %d variables', count($rows)), 0);

            foreach ($rows as $row) {
                if ($budget <= 0) {
                    $this->SendDebug('Sync', 'Value budget for this run exhausted, continuing next run', 0);
                    break;
                }

                $variableID = intval($row['VariableID']);
                if (!IPS_VariableExists($variableID) || !AC_GetLoggingStatus($archiveID, $variableID)) {
                    continue;
                }

                // New variable: decide where to start
                if (!isset($state[$variableID])) {
                    if ($initialSync == 1) {
                        $state[$variableID] = $now;
                        $this->writeSyncState($state);
                        continue;
                    }
                    $state[$variableID] = -1;
                }

                $prefix = $this->buildSeriesKey($row);
                $lines = [];
                $synced = $state[$variableID];
                $start = $synced + 1;
                $window = self::WINDOW_INITIAL;
                $count = 0;

                while ($start <= $now && $budget > 0) {
                    $end = min($start + $window - 1, $now);
                    $values = AC_GetLoggedValues($archiveID, $variableID, $start, $end, self::AC_LIMIT);

                    // Window too large for one call -> shrink and retry
                    if (count($values) >= self::AC_LIMIT && $window > 1) {
                        $window = max(1, intdiv($window, 4));
                        continue;
                    }

                    // Archive returns newest first. The cursor is the timestamp of the last
                    // transferred value (as in SyncMySQL), not the window end, so values that
                    // reach the archive later with an older timestamp are still picked up.
                    foreach (array_reverse($values) as $value) {
                        $timestamp = intval($value['TimeStamp']);
                        $line = $this->buildLine($prefix, $value['Value'], $timestamp);
                        if ($line !== null) {
                            $lines[] = $line;
                        }
                        $synced = max($synced, $timestamp);
                    }
                    $budget -= count($values);
                    $count += count($values);

                    if (count($lines) >= $batchSize) {
                        $this->writeLines($lines);
                        $lines = [];
                        $state[$variableID] = $synced;
                        $this->writeSyncState($state);
                    }

                    // Grow the window on sparse data, keep it on dense data
                    if (count($values) == 0) {
                        $window = min($window * 4, self::WINDOW_MAX);
                    } elseif (count($values) < self::AC_LIMIT / 4) {
                        $window = min($window * 2, self::WINDOW_MAX);
                    }
                    $start = $end + 1;
                }

                if (count($lines) > 0) {
                    $this->writeLines($lines);
                }
                $state[$variableID] = $synced;
                $this->writeSyncState($state);
                $total += $count;

                if ($count > 0) {
                    $this->SendDebug('Sync', sprintf('Transferred %d values of variable #%d', $count, $variableID), 0);
                }
            }

            $this->SendDebug('Sync', sprintf('Done. %d values transferred', $total), 0);
            $this->setConnectionOK();
        } catch (Exception $e) {
            $this->setConnectionError($e->getMessage());
            $this->SendDebug('Sync', 'Error: ' . $e->getMessage(), 0);
            $this->LogMessage('Sync failed: ' . $e->getMessage(), KL_ERROR);
        } finally {
            IPS_SemaphoreLeave('SINF_Sync' . $this->InstanceID);
        }
    }

    /**
     * Check reachability, credentials and bucket/database.
     */
    public function TestConnection()
    {
        try {
            $bucket = $this->ReadPropertyString('Bucket');
            switch ($this->ReadPropertyInteger('APIVersion')) {
                case 1:
                    $result = $this->httpRequest('GET', '/query', ['q' => 'SHOW DATABASES']);
                    $this->expectStatus($result, [200]);
                    $json = json_decode($result['body'], true);
                    $databases = array_column($json['results'][0]['series'][0]['values'] ?? [], 0);
                    if (!in_array($bucket, $databases)) {
                        throw new Exception(sprintf($this->Translate('Database "%s" does not exist'), $bucket));
                    }
                    break;
                case 2:
                    $result = $this->httpRequest('GET', '/api/v2/buckets', ['name' => $bucket, 'org' => $this->ReadPropertyString('Organization')]);
                    $this->expectStatus($result, [200]);
                    $json = json_decode($result['body'], true);
                    if (count($json['buckets'] ?? []) == 0) {
                        throw new Exception(sprintf($this->Translate('Bucket "%s" does not exist'), $bucket));
                    }
                    break;
                case 3:
                    // Databases are created on first write in InfluxDB 3
                    $result = $this->httpRequest('GET', '/health');
                    $this->expectStatus($result, [200]);
                    break;
            }
            $this->setConnectionOK();
            echo $this->Translate('Connection OK');
        } catch (Exception $e) {
            $this->setConnectionError($e->getMessage());
            echo $this->Translate('Error') . ': ' . $e->getMessage();
        }
    }

    /**
     * Re-transfer data from $Timestamp on. VariableID 0 resets all variables.
     * Timestamp 0 re-transfers the complete archive.
     */
    /**
     * Add all variables below $ParentID to the list (scriptable version of "Add multiple variables").
     * Returns a status message.
     */
    public function AddVariables(int $ParentID, bool $Recursive, bool $OnlyLogged)
    {
        return $this->bulkAddVariables($ParentID, $Recursive, $OnlyLogged, 0, '');
    }

    public function ResetSyncState(int $VariableID, int $Timestamp)
    {
        $state = $this->readSyncState();
        foreach ($this->getActiveRows() as $row) {
            $id = intval($row['VariableID']);
            if ($VariableID == 0 || $VariableID == $id) {
                $state[$id] = $Timestamp - 1;
            }
        }
        $this->writeSyncState($state);
        $this->UpdateFormField('Variables', 'values', json_encode($this->getVariableListValues()));
    }

    /**
     * VariableID => last synced unix timestamp
     */
    public function GetSyncState(): array
    {
        return $this->readSyncState();
    }

    // ---------------------------------------------------------------------
    // Bulk add
    // ---------------------------------------------------------------------

    private function bulkAddVariables(int $parentID, bool $recursive, bool $onlyLogged, int $defaultAggregation, string $defaultUnit): string
    {
        if ($parentID <= 0 || !IPS_ObjectExists($parentID)) {
            return $this->Translate('Please select a category or instance');
        }

        $archiveID = IPS_GetInstanceListByModuleID(self::ARCHIVE_GUID)[0] ?? 0;
        $knownUnits = $this->getUnitOptions();

        $rows = $this->getRows();
        $listed = array_flip(array_map(function ($row)
        {
            return intval($row['VariableID'] ?? 0);
        }, $rows));

        $added = 0;
        $skipped = 0;
        foreach ($this->collectVariables($parentID, $recursive) as $variableID => $path) {
            if (isset($listed[$variableID])) {
                $skipped++;
                continue;
            }
            $logged = ($archiveID > 0) && AC_GetLoggingStatus($archiveID, $variableID);
            if ($onlyLogged && !$logged) {
                continue;
            }
            // Strings cannot be stored as float values
            if (IPS_GetVariable($variableID)['VariableType'] == VARIABLETYPE_STRING) {
                continue;
            }

            $unit = $this->unitFromPresentation($variableID);
            if (!in_array($unit, $knownUnits, true)) {
                $unit = $defaultUnit;
            }

            $rows[] = [
                'Active'          => true,
                'Ident'           => $path,
                'VariableID'      => $variableID,
                'AggregationType' => $logged ? AC_GetAggregationType($archiveID, $variableID) : $defaultAggregation,
                'Unit'            => $unit,
                'Meter'           => '',
                'Tariff'          => '',
                'Zone'            => '',
                'UsageType'       => '',
                'IdentLegacy'     => '',
                'Measurement'     => '',
                'Tags'            => '',
                'Description'     => ''
            ];
            $listed[$variableID] = true;
            $added++;
        }

        if ($added > 0) {
            // User-triggered: store the extended list and apply it
            IPS_SetProperty($this->InstanceID, 'Variables', json_encode($rows));
            IPS_ApplyChanges($this->InstanceID);
            $this->ReloadForm();
        }

        return sprintf($this->Translate('%d variables added, %d skipped (already listed)'), $added, $skipped);
    }

    /**
     * VariableID => identification path relative to the selected object, e.g. "AHU1/Supply temperature"
     */
    private function collectVariables(int $parentID, bool $recursive, string $prefix = ''): array
    {
        $result = [];
        $children = IPS_GetChildrenIDs($parentID);
        usort($children, function ($a, $b)
        {
            return IPS_GetObject($a)['ObjectPosition'] <=> IPS_GetObject($b)['ObjectPosition'] ?: strcmp(IPS_GetName($a), IPS_GetName($b));
        });
        foreach ($children as $childID) {
            // Never pick up our own links/children
            if ($childID == $this->InstanceID) {
                continue;
            }
            $name = $prefix . IPS_GetName($childID);
            if (IPS_VariableExists($childID)) {
                $result[$childID] = $name;
            }
            if ($recursive && IPS_HasChildren($childID) && !IPS_LinkExists($childID)) {
                $result += $this->collectVariables($childID, true, $name . '/');
            }
        }
        // A variable itself may be selected
        if ($prefix === '' && IPS_VariableExists($parentID)) {
            $result = [$parentID => IPS_GetName($parentID)] + $result;
        }
        return $result;
    }

    private function unitFromPresentation(int $variableID): string
    {
        $variable = IPS_GetVariable($variableID);

        // Symcon 8 presentations
        foreach (['VariableCustomPresentation', 'VariablePresentation'] as $key) {
            $presentation = $variable[$key] ?? [];
            if (is_array($presentation) && isset($presentation['SUFFIX']) && trim((string) $presentation['SUFFIX']) !== '') {
                return trim((string) $presentation['SUFFIX']);
            }
        }

        // Legacy profiles
        $profile = $variable['VariableCustomProfile'] !== '' ? $variable['VariableCustomProfile'] : $variable['VariableProfile'];
        if ($profile !== '' && IPS_VariableProfileExists($profile)) {
            return trim(IPS_GetVariableProfile($profile)['Suffix']);
        }
        return '';
    }

    private function getUnitOptions(): array
    {
        $form = json_decode(file_get_contents(__DIR__ . '/form.json'), true);
        $list = $this->findElement($form['elements'], 'Variables');
        foreach ($list['columns'] as $column) {
            if ($column['name'] == 'Unit') {
                return array_column($column['edit']['options'], 'value');
            }
        }
        return [];
    }

    // ---------------------------------------------------------------------
    // Line protocol
    // ---------------------------------------------------------------------

    private function buildSeriesKey(array $row): string
    {
        $measurement = trim($row['Measurement'] ?? '') !== '' ? trim($row['Measurement']) : $this->ReadPropertyString('Measurement');

        $tags = [
            'ident'       => $row['Ident'] ?? '',
            'variableid'  => (string) intval($row['VariableID']),
            'unit'        => $row['Unit'] ?? '',
            'aggregation' => (intval($row['AggregationType'] ?? 0) == 1) ? 'counter' : 'average',
            'tariff'      => $row['Tariff'] ?? '',
            'usage_type'  => $row['UsageType'] ?? '',
            'ident_legacy'=> $row['IdentLegacy'] ?? ''
        ];

        $meter = $this->findMeter($row['Meter'] ?? '');
        if ($meter !== null) {
            $tags['meter'] = $meter['Number'];
            $tags['meter_bks'] = $meter['BKS'] ?? '';
        }

        $zone = $this->findZone($row['Zone'] ?? '');
        if ($zone !== null) {
            $tags['zone'] = $zone['Location'];
            $tags['economic_unit'] = $zone['EconomicUnit'] ?? '';
            $tags['building_unit'] = $zone['BuildingUnit'] ?? '';
        }

        $format = $this->ReadPropertyInteger('Format');
        if ($format > 0 && $this->ReadPropertyBoolean('FormatTags')) {
            if (preg_match('/' . $this->identFormats[$format] . '/', $row['Ident'] ?? '', $matches)) {
                foreach ($this->formatTagKeys as $group => $key) {
                    $tags[$key] = $matches[$group] ?? '';
                }
            }
        }

        // Custom tags win over generated ones
        foreach ($this->parseExtraTags($row['Tags'] ?? '') as $key => $value) {
            $tags[$key] = $value;
        }

        // Sorted tags are the fastest path for InfluxDB, empty values are not allowed
        ksort($tags, SORT_STRING);
        $key = $this->escapeMeasurement($measurement);
        foreach ($tags as $tagKey => $tagValue) {
            $tagValue = trim((string) $tagValue);
            if ($tagValue === '') {
                continue;
            }
            $key .= ',' . $this->escapeTag((string) $tagKey) . '=' . $this->escapeTag($tagValue);
        }
        return $key;
    }

    private function buildLine(string $seriesKey, $value, int $timestamp): ?string
    {
        if (is_bool($value)) {
            $value = $value ? 1.0 : 0.0;
        }
        if (!is_numeric($value)) {
            return null;
        }
        $value = (float) $value;
        if (!is_finite($value)) {
            return null;
        }
        // Always a float field: avoids field type conflicts between int and float variables
        $formatted = json_encode($value);
        if (strpos($formatted, '.') === false && stripos($formatted, 'e') === false) {
            $formatted .= '.0';
        }
        return $seriesKey . ' value=' . $formatted . ' ' . $timestamp;
    }

    private function parseExtraTags(string $tags): array
    {
        $result = [];
        foreach (explode(',', $tags) as $pair) {
            $parts = explode('=', $pair, 2);
            if (count($parts) == 2 && trim($parts[0]) !== '' && trim($parts[1]) !== '') {
                $result[trim($parts[0])] = trim($parts[1]);
            }
        }
        return $result;
    }

    private function escapeMeasurement(string $value): string
    {
        $value = str_replace(["\r", "\n"], ' ', $value);
        return str_replace([',', ' '], ['\,', '\ '], rtrim($value, '\\'));
    }

    private function escapeTag(string $value): string
    {
        $value = str_replace(["\r", "\n"], ' ', $value);
        return str_replace([',', '=', ' '], ['\,', '\=', '\ '], rtrim($value, '\\'));
    }

    // ---------------------------------------------------------------------
    // HTTP
    // ---------------------------------------------------------------------

    private function writeLines(array $lines)
    {
        $body = implode("\n", $lines);
        $bucket = $this->ReadPropertyString('Bucket');

        switch ($this->ReadPropertyInteger('APIVersion')) {
            case 1:
                $path = '/write';
                $query = ['db' => $bucket, 'precision' => 's'];
                break;
            case 2:
                $path = '/api/v2/write';
                $query = ['org' => $this->ReadPropertyString('Organization'), 'bucket' => $bucket, 'precision' => 's'];
                break;
            case 3:
                $path = '/api/v3/write_lp';
                $query = ['db' => $bucket, 'precision' => 'second'];
                break;
            default:
                throw new Exception('Unknown API version');
        }

        $headers = ['Content-Type: text/plain; charset=utf-8'];
        if ($this->ReadPropertyBoolean('Compress') && function_exists('gzencode')) {
            $body = gzencode($body);
            $headers[] = 'Content-Encoding: gzip';
        }

        $result = $this->httpRequest('POST', $path, $query, $body, $headers);
        $this->expectStatus($result, [200, 204]);
        $this->SendDebug('Write', sprintf('%d lines -> HTTP %d', count($lines), $result['code']), 0);
    }

    private function httpRequest(string $method, string $path, array $query = [], ?string $body = null, array $headers = []): array
    {
        $url = rtrim($this->ReadPropertyString('URL'), '/') . $path;
        if (count($query) > 0) {
            $url .= '?' . http_build_query($query);
        }

        $headers = array_merge($headers, $this->authHeaders());

        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 5);
        curl_setopt($ch, CURLOPT_TIMEOUT, 60);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }
        if ($this->ReadPropertyBoolean('SSLDontVerify')) {
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
        }

        $response = curl_exec($ch);
        if ($response === false) {
            $error = curl_error($ch);
            curl_close($ch);
            throw new Exception($error);
        }
        $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        return ['code' => $code, 'body' => (string) $response];
    }

    private function authHeaders(): array
    {
        $token = $this->ReadPropertyString('Token');
        switch ($this->ReadPropertyInteger('APIVersion')) {
            case 1:
                $user = $this->ReadPropertyString('Username');
                if ($user !== '') {
                    return ['Authorization: Basic ' . base64_encode($user . ':' . $this->ReadPropertyString('Password'))];
                }
                return [];
            case 2:
                return ($token !== '') ? ['Authorization: Token ' . $token] : [];
            case 3:
                return ($token !== '') ? ['Authorization: Bearer ' . $token] : [];
        }
        return [];
    }

    private function expectStatus(array $result, array $accepted)
    {
        if (!in_array($result['code'], $accepted)) {
            $message = $result['body'];
            $json = json_decode($result['body'], true);
            if (is_array($json)) {
                $message = $json['message'] ?? $json['error'] ?? $result['body'];
            }
            throw new Exception(sprintf('HTTP %d: %s', $result['code'], substr(trim((string) $message), 0, 300)));
        }
    }

    // ---------------------------------------------------------------------
    // Configuration helpers
    // ---------------------------------------------------------------------

    private function validateConfiguration(): string
    {
        if (!filter_var($this->ReadPropertyString('URL'), FILTER_VALIDATE_URL)) {
            return $this->Translate('URL is invalid');
        }
        if (trim($this->ReadPropertyString('Bucket')) === '') {
            return $this->Translate('Bucket / Database is missing');
        }
        if (trim($this->ReadPropertyString('Measurement')) === '') {
            return $this->Translate('Measurement is missing');
        }
        if ($this->ReadPropertyInteger('APIVersion') == 2) {
            if ($this->ReadPropertyString('Organization') === '' || $this->ReadPropertyString('Token') === '') {
                return $this->Translate('Organization and token are required for InfluxDB 2.x');
            }
        }
        $seen = [];
        foreach ($this->getRows() as $row) {
            $id = intval($row['VariableID']);
            if ($id > 0 && isset($seen[$id])) {
                return sprintf($this->Translate('Variable #%d is listed more than once'), $id);
            }
            $seen[$id] = true;
        }
        return '';
    }

    private function getRows(): array
    {
        $rows = json_decode($this->ReadPropertyString('Variables'), true);
        return is_array($rows) ? $rows : [];
    }

    private function getActiveRows(): array
    {
        $result = [];
        foreach ($this->getRows() as $row) {
            $id = intval($row['VariableID'] ?? 0);
            if (($row['Active'] ?? false) && $id > 0 && !isset($result[$id])) {
                $result[$id] = $row;
            }
        }
        return array_values($result);
    }

    private function findMeter(string $number): ?array
    {
        if ($number === '') {
            return null;
        }
        foreach (json_decode($this->ReadPropertyString('Meters'), true) ?: [] as $meter) {
            if (($meter['Number'] ?? '') === $number) {
                return $meter;
            }
        }
        return null;
    }

    private function findZone(string $location): ?array
    {
        if ($location === '') {
            return null;
        }
        foreach (json_decode($this->ReadPropertyString('Zones'), true) ?: [] as $zone) {
            if (($zone['Location'] ?? '') === $location) {
                return $zone;
            }
        }
        return null;
    }

    private function getMeterOptions(): array
    {
        $options = [];
        foreach (json_decode($this->ReadPropertyString('Meters'), true) ?: [] as $meter) {
            if (($meter['Number'] ?? '') !== '') {
                $options[] = ['caption' => sprintf('%s (%s)', $meter['Number'], $meter['BKS'] ?? ''), 'value' => $meter['Number']];
            }
        }
        usort($options, function ($a, $b)
        {
            return strcmp($a['caption'], $b['caption']);
        });
        array_unshift($options, ['caption' => $this->Translate('(none)'), 'value' => '']);
        return $options;
    }

    private function getZoneOptions(): array
    {
        $options = [];
        foreach (json_decode($this->ReadPropertyString('Zones'), true) ?: [] as $zone) {
            if (($zone['Location'] ?? '') !== '') {
                $options[] = ['caption' => sprintf('%s (%sm²)', $zone['Location'], $zone['Area'] ?? 0), 'value' => $zone['Location']];
            }
        }
        usort($options, function ($a, $b)
        {
            return strcmp($a['caption'], $b['caption']);
        });
        array_unshift($options, ['caption' => $this->Translate('(none)'), 'value' => '']);
        return $options;
    }

    private function getVariableListValues(): array
    {
        $format = $this->identFormats[$this->ReadPropertyInteger('Format')];
        $state = $this->readSyncState();
        $archiveIDs = IPS_GetInstanceListByModuleID(self::ARCHIVE_GUID);
        $archiveID = $archiveIDs[0] ?? 0;
        $seen = [];

        $values = [];
        foreach ($this->getRows() as $row) {
            $id = intval($row['VariableID'] ?? 0);
            $status = $this->Translate('OK');
            if (!IPS_VariableExists($id)) {
                $status = $this->Translate('Variable missing');
            } elseif (isset($seen[$id])) {
                $status = $this->Translate('Duplicate');
            } elseif ($format !== '' && !preg_match('/' . $format . '/', $row['Ident'] ?? '')) {
                $status = $this->Translate('Format mismatch');
            } elseif ($archiveID > 0 && !AC_GetLoggingStatus($archiveID, $id)) {
                $status = $this->Translate('Not logged');
            }
            $seen[$id] = true;

            $row['State'] = $status;
            $row['LastSync'] = (isset($state[$id]) && $state[$id] > 0) ? date('d.m.Y H:i:s', $state[$id]) : '-';
            $values[] = $row;
        }
        return $values;
    }

    private function readSyncState(): array
    {
        $state = json_decode($this->ReadAttributeString('SyncState'), true);
        if (!is_array($state)) {
            return [];
        }
        // JSON object keys come back as strings
        $result = [];
        foreach ($state as $id => $timestamp) {
            $result[intval($id)] = intval($timestamp);
        }
        return $result;
    }

    private function writeSyncState(array $state)
    {
        $this->WriteAttributeString('SyncState', json_encode((object) $state));
    }

    private function pruneSyncState()
    {
        $ids = array_map(function ($row)
        {
            return intval($row['VariableID'] ?? 0);
        }, $this->getRows());
        $state = $this->readSyncState();
        $pruned = array_intersect_key($state, array_flip($ids));
        if (count($pruned) != count($state)) {
            $this->writeSyncState($pruned);
        }
    }

    private function setConnectionOK()
    {
        $this->SetBuffer('ConnectionStatus', '');
        if ($this->ReadPropertyBoolean('Active') && $this->GetStatus() == self::STATUS_CONNECTION_ERROR) {
            $this->SetStatus(IS_ACTIVE);
        }
        $this->updateStatusLabel();
    }

    private function setConnectionError(string $message)
    {
        $this->SetBuffer('ConnectionStatus', $message);
        if ($this->ReadPropertyBoolean('Active')) {
            $this->SetStatus(self::STATUS_CONNECTION_ERROR);
        }
        $this->updateStatusLabel();
    }

    private function updateStatusLabel()
    {
        $error = $this->GetBuffer('ConnectionStatus');
        $this->UpdateFormField('ConnectionStatus', 'visible', $error !== '');
        $this->UpdateFormField('ConnectionStatus', 'caption', $this->Translate('Error') . ': ' . $error);
    }

    private function versionFieldVisibility(int $apiVersion): array
    {
        return [
            'Organization' => $apiVersion == 2,
            'Token'        => $apiVersion != 1,
            'Username'     => $apiVersion == 1,
            'Password'     => $apiVersion == 1
        ];
    }

    private function updateVersionFields(int $apiVersion)
    {
        foreach ($this->versionFieldVisibility($apiVersion) as $name => $visible) {
            $this->UpdateFormField($name, 'visible', $visible);
        }
    }

    private function &findElement(array &$elements, string $name)
    {
        foreach ($elements as &$element) {
            if (($element['name'] ?? '') === $name) {
                return $element;
            }
            if (isset($element['items'])) {
                $found = &$this->findElement($element['items'], $name);
                if ($found !== null) {
                    return $found;
                }
            }
        }
        $null = null;
        return $null;
    }

    private function patchElement(array &$elements, string $name, array $patch)
    {
        $element = &$this->findElement($elements, $name);
        if ($element !== null) {
            foreach ($patch as $key => $value) {
                $element[$key] = $value;
            }
        }
    }

    // ---------------------------------------------------------------------
    // Symcon object maintenance (same behaviour as SyncMySQL)
    // ---------------------------------------------------------------------

    private function UpdateArchive()
    {
        if (!$this->ReadPropertyBoolean('Active') || !$this->ReadPropertyBoolean('AutoEnableLogging')) {
            return;
        }
        $archiveID = IPS_GetInstanceListByModuleID(self::ARCHIVE_GUID)[0];
        $needApplyChanges = false;
        foreach ($this->getActiveRows() as $row) {
            $id = intval($row['VariableID']);
            if (!IPS_VariableExists($id)) {
                continue;
            }
            if (!AC_GetLoggingStatus($archiveID, $id)) {
                AC_SetLoggingStatus($archiveID, $id, true);
                $needApplyChanges = true;
            }
            $type = intval($row['AggregationType'] ?? 0);
            if (AC_GetAggregationType($archiveID, $id) != $type) {
                AC_SetAggregationType($archiveID, $id, $type);
                $needApplyChanges = true;
            }
        }
        if ($needApplyChanges) {
            IPS_ApplyChanges($archiveID);
        }
    }

    private function UpdateLinks()
    {
        $idList = [];
        foreach ($this->getRows() as $row) {
            $id = intval($row['VariableID'] ?? 0);
            if (!IPS_VariableExists($id)) {
                continue;
            }
            $idList[] = $id;

            $lid = @IPS_GetObjectIDByIdent('Link' . $id, $this->InstanceID);
            if (!$lid) {
                $lid = IPS_CreateLink();
                IPS_SetParent($lid, $this->InstanceID);
                IPS_SetIdent($lid, 'Link' . $id);
                IPS_SetLinkTargetID($lid, $id);
            }
            $name = (($row['Ident'] ?? '') !== '' ? $row['Ident'] . ' ' : '') . '(' . IPS_GetName($id) . ')';
            if (IPS_GetName($lid) != $name) {
                IPS_SetName($lid, $name);
            }
        }

        foreach (IPS_GetChildrenIDs($this->InstanceID) as $child) {
            if (IPS_LinkExists($child) && !in_array(IPS_GetLink($child)['TargetID'], $idList)) {
                IPS_DeleteLink($child);
            }
        }
    }
}
