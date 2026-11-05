<?php

declare(strict_types=1);

include_once __DIR__ . '/stubs/autoload.php';

use PHPUnit\Framework\TestCase;

/**
 * Functional tests against a mock InfluxDB HTTP server (tests/mock/influx.php).
 */
class SyncInfluxDBTest extends TestCase
{
    private const ARCHIVE_GUID = '{43192F0B-135B-4CE7-A0A7-1475603F3060}';
    private const MODULE_GUID = '{20D2A412-2EBB-41E1-BB50-90E251E0FF23}';
    private const BKS_IDENT = 'SEB-HS-B1-R101=001+B1-R101-12W3/T-IST-AI-----';

    private static $server;
    private static $port;
    private static $mockDir;

    private $archiveID;

    public static function setUpBeforeClass(): void
    {
        self::$mockDir = sys_get_temp_dir() . '/sinf_mock_' . getmypid();
        @mkdir(self::$mockDir);
        self::$port = 18000 + (getmypid() % 1000);
        self::$server = proc_open(
            [PHP_BINARY, '-S', '127.0.0.1:' . self::$port, __DIR__ . '/mock/influx.php'],
            [['pipe', 'r'], ['file', '/dev/null', 'w'], ['file', '/dev/null', 'w']],
            $pipes,
            null,
            ['MOCK_DIR' => self::$mockDir]
        );
        // Wait for the server socket
        for ($i = 0; $i < 50; $i++) {
            $socket = @fsockopen('127.0.0.1', self::$port);
            if ($socket) {
                fclose($socket);
                break;
            }
            usleep(100000);
        }
    }

    public static function tearDownAfterClass(): void
    {
        proc_terminate(self::$server);
        array_map('unlink', glob(self::$mockDir . '/*'));
        @rmdir(self::$mockDir);
    }

    protected function setUp(): void
    {
        IPS\Kernel::reset();
        IPS\ModuleLoader::loadLibrary(__DIR__ . '/stubs/CoreStubs/library.json');
        IPS\ModuleLoader::loadLibrary(__DIR__ . '/../library.json');
        $this->archiveID = IPS_CreateInstance(self::ARCHIVE_GUID);
        @unlink(self::$mockDir . '/requests.jsonl');
        $this->setMockStatus(204);
        parent::setUp();
    }

    public function testBackfillDenseAndSparseHistory(): void
    {
        $now = time();
        // 25.000 values every 10 s -> forces the read window to shrink below the 10.000 row limit
        $dense = $this->createLoggedVariable(VARIABLETYPE_FLOAT, $this->series($now - 3600 - 250000, 10, 25000));
        // 50 values in 2015 -> forces the read window to grow from 1970
        $sparse = $this->createLoggedVariable(VARIABLETYPE_INTEGER, $this->series(mktime(0, 0, 0, 1, 1, 2015), 7 * 86400, 50));
        $bool = $this->createLoggedVariable(VARIABLETYPE_BOOLEAN, [
            ['TimeStamp' => $now - 500, 'Value' => true],
            ['TimeStamp' => $now - 400, 'Value' => false]
        ]);

        $id = $this->createInstance([], [$this->row($dense), $this->row($sparse), $this->row($bool)]);
        SINF_Sync($id);

        $points = $this->writtenPoints();
        $this->assertCount(25000, $points[$dense] ?? []);
        $this->assertCount(50, $points[$sparse] ?? []);
        $this->assertCount(2, $points[$bool] ?? []);

        // Every timestamp exactly once and in ascending order
        $timestamps = array_column($points[$dense], 'ts');
        $this->assertEquals(count($timestamps), count(array_unique($timestamps)));
        $sorted = $timestamps;
        sort($sorted);
        $this->assertSame($sorted, $timestamps);

        $this->assertSame([1.0, 0.0], array_column($points[$bool], 'value'));
        $this->assertEquals(IS_ACTIVE, IPS_GetInstance($id)['InstanceStatus']);

        // Request shape for v2
        $write = $this->writeRequests()[0];
        $this->assertSame('/api/v2/write', $write['path']);
        $this->assertSame(['org' => 'ines', 'bucket' => 'symcon', 'precision' => 's'], $write['query']);
        $this->assertSame('Token secret', $write['auth']);
        $this->assertTrue($write['gzip']);

        // Second run has nothing to do
        $before = count($this->writeRequests());
        SINF_Sync($id);
        $this->assertCount($before, $this->writeRequests());
    }

    public function testIncrementalSync(): void
    {
        $now = time();
        $var = $this->createLoggedVariable(VARIABLETYPE_FLOAT, $this->series($now - 5000, 60, 10));
        $id = $this->createInstance([], [$this->row($var)]);
        SINF_Sync($id);
        $this->assertCount(10, $this->writtenPoints()[$var]);

        AC_AddLoggedValues($this->archiveID, $var, $this->series($now - 1000, 1, 3));
        $this->clearRequests();
        SINF_Sync($id);
        $points = $this->writtenPoints()[$var];
        $this->assertCount(3, $points);
        $this->assertSame($now - 1000, $points[0]['ts']);
    }

    public function testFailedWriteDoesNotAdvanceState(): void
    {
        $now = time();
        $var = $this->createLoggedVariable(VARIABLETYPE_FLOAT, $this->series($now - 5000, 60, 10));
        $id = $this->createInstance([], [$this->row($var)]);

        $this->setMockStatus(500);
        SINF_Sync($id);
        $this->assertEquals(200, IPS_GetInstance($id)['InstanceStatus']);
        $this->assertSame([], SINF_GetSyncState($id));

        $this->setMockStatus(204);
        $this->clearRequests();
        SINF_Sync($id);
        $this->assertCount(10, $this->writtenPoints()[$var]);
        $this->assertEquals(IS_ACTIVE, IPS_GetInstance($id)['InstanceStatus']);
    }

    public function testBudgetSplitsBackfillWithoutGaps(): void
    {
        $now = time();
        $var = $this->createLoggedVariable(VARIABLETYPE_FLOAT, $this->series($now - 100000, 2, 45000));
        $id = $this->createInstance(['MaxValuesPerRun' => 10000], [$this->row($var)]);

        $runs = 0;
        do {
            $before = count($this->writtenPoints()[$var] ?? []);
            SINF_Sync($id);
            $runs++;
        } while (count($this->writtenPoints()[$var] ?? []) > $before && $runs < 20);

        $timestamps = array_column($this->writtenPoints()[$var], 'ts');
        $this->assertCount(45000, array_unique($timestamps));
        $this->assertGreaterThanOrEqual(4, $runs);
    }

    public function testResetSyncStateResendsHistory(): void
    {
        $now = time();
        $var = $this->createLoggedVariable(VARIABLETYPE_FLOAT, $this->series($now - 5000, 60, 10));
        $id = $this->createInstance([], [$this->row($var)]);
        SINF_Sync($id);

        $this->clearRequests();
        SINF_ResetSyncState($id, 0, $now - 5000 + 60 * 5);
        SINF_Sync($id);
        $this->assertCount(5, $this->writtenPoints()[$var]);
    }

    public function testInitialSyncFromNowSkipsHistory(): void
    {
        $now = time();
        $var = $this->createLoggedVariable(VARIABLETYPE_FLOAT, $this->series($now - 5000, 60, 10));
        $id = $this->createInstance(['InitialSync' => 1], [$this->row($var)]);
        SINF_Sync($id);
        $this->assertCount(0, $this->writeRequests());
    }

    public function testTagsAndEscaping(): void
    {
        $now = time();
        $var = $this->createLoggedVariable(VARIABLETYPE_FLOAT, [['TimeStamp' => $now - 100, 'Value' => 21.5]]);
        $row = $this->row($var, [
            'Ident'           => self::BKS_IDENT,
            'Unit'            => '°C',
            'AggregationType' => 1,
            'Meter'           => 'M 01',
            'Zone'            => 'Haus D',
            'Measurement'     => 'heating, flow',
            'Tags'            => 'brick=urn:ines#AHU1_SAT, site = Offenburg'
        ]);
        $id = $this->createInstance([
            'Format' => 1,
            'Meters' => json_encode([['Number' => 'M 01', 'BKS' => 'SEB-HS-B1-R101=001+B1-R101-12W3', 'Description' => '', 'Comment' => '']]),
            'Zones'  => json_encode([['Location' => 'Haus D', 'Area' => 1200, 'NetArea' => 1000, 'EconomicUnit' => 'WE1', 'BuildingUnit' => '', 'BuildingAssignment' => '', 'ConstructionDate' => '2010']])
        ], [$row]);
        SINF_Sync($id);

        $line = $this->writeRequests()[0]['body'];
        $this->assertStringContainsString('heating\,\ flow,', $line);
        $this->assertStringContainsString(',aggregation=counter,', $line);
        $this->assertStringContainsString(',brick=urn:ines#AHU1_SAT,', $line);
        $this->assertStringContainsString(',site=Offenburg', $line);
        $this->assertStringContainsString(',meter=M\ 01,', $line);
        $this->assertStringContainsString(',zone=Haus\ D', $line);
        $this->assertStringContainsString(',ident=SEB-HS-B1-R101\=001+B1-R101-12W3/T-IST-AI-----,', $line);
        $this->assertStringContainsString(',data_point=IST,', $line);
        $this->assertStringContainsString(',additional_field_01=AI,', $line);
        $this->assertStringContainsString(',function_identifier=T,', $line);
        $this->assertStringContainsString(',unit=°C,', $line);
        $this->assertStringContainsString(' value=21.5 ' . ($now - 100), $line);
        // Empty BKS groups are not written as empty tags
        $this->assertTrue(strpos($line, 'additional_field_02') === false);
    }

    public function testInfluxDB1And3Endpoints(): void
    {
        $now = time();
        $var = $this->createLoggedVariable(VARIABLETYPE_FLOAT, [['TimeStamp' => $now - 100, 'Value' => 1]]);

        $id = $this->createInstance(['APIVersion' => 1, 'Username' => 'u', 'Password' => 'p', 'Compress' => false], [$this->row($var)]);
        SINF_Sync($id);
        $request = $this->writeRequests()[0];
        $this->assertSame('/write', $request['path']);
        $this->assertSame(['db' => 'symcon', 'precision' => 's'], $request['query']);
        $this->assertSame('Basic ' . base64_encode('u:p'), $request['auth']);
        $this->assertTrue($request['gzip'] === false);

        $this->clearRequests();
        $id = $this->createInstance(['APIVersion' => 3, 'Organization' => ''], [$this->row($var)]);
        SINF_Sync($id);
        $request = $this->writeRequests()[0];
        $this->assertSame('/api/v3/write_lp', $request['path']);
        $this->assertSame(['db' => 'symcon', 'precision' => 'second'], $request['query']);
        $this->assertSame('Bearer secret', $request['auth']);
    }

    public function testConfigurationErrors(): void
    {
        $var = $this->createLoggedVariable(VARIABLETYPE_FLOAT, []);
        $id = $this->createInstance([], [$this->row($var), $this->row($var)]);
        $this->assertEquals(201, IPS_GetInstance($id)['InstanceStatus']);

        $id = $this->createInstance(['Organization' => ''], [$this->row($var)]);
        $this->assertEquals(201, IPS_GetInstance($id)['InstanceStatus']);
    }

    public function testConfigurationFormAndLinks(): void
    {
        $var = $this->createLoggedVariable(VARIABLETYPE_FLOAT, []);
        $id = $this->createInstance([], [$this->row($var, ['Ident' => 'AHU1 supply temp'])]);

        $form = json_decode(IPS\InstanceManager::getInstanceInterface($id)->GetConfigurationForm(), true);
        $this->assertIsArray($form);
        $children = IPS_GetChildrenIDs($id);
        $this->assertCount(1, $children);
        $this->assertSame($var, IPS_GetLink($children[0])['TargetID']);
    }

    public function testTestConnection(): void
    {
        $id = $this->createInstance([], []);
        ob_start();
        SINF_TestConnection($id);
        $this->assertSame('Connection OK', ob_get_clean());

        $id = $this->createInstance(['Bucket' => 'missing'], []);
        ob_start();
        SINF_TestConnection($id);
        $this->assertStringContainsString('does not exist', ob_get_clean());
    }

    // ------------------------------------------------------------------

    private function createInstance(array $properties, array $rows): int
    {
        $id = IPS_CreateInstance(self::MODULE_GUID);
        $config = array_merge([
            'Active'            => true,
            'APIVersion'        => 2,
            'URL'               => 'http://127.0.0.1:' . self::$port,
            'Organization'      => 'ines',
            'Bucket'            => 'symcon',
            'Token'             => 'secret',
            'AutoEnableLogging' => true,
            'Variables'         => json_encode($rows)
        ], $properties);
        IPS_SetConfiguration($id, json_encode($config));
        IPS_ApplyChanges($id);
        return $id;
    }

    private function createLoggedVariable(int $type, array $values): int
    {
        $id = IPS_CreateVariable($type);
        AC_SetLoggingStatus($this->archiveID, $id, true);
        if (count($values) > 0) {
            AC_AddLoggedValues($this->archiveID, $id, $values);
        }
        return $id;
    }

    private function row(int $variableID, array $overrides = []): array
    {
        return array_merge([
            'Active'          => true,
            'VariableID'      => $variableID,
            'Ident'           => 'Var' . $variableID,
            'AggregationType' => 0,
            'Unit'            => 'kWh',
            'Meter'           => '',
            'Tariff'          => '',
            'Zone'            => '',
            'UsageType'       => '',
            'IdentLegacy'     => '',
            'Measurement'     => '',
            'Tags'            => '',
            'Description'     => ''
        ], $overrides);
    }

    private function series(int $start, int $step, int $count): array
    {
        $values = [];
        for ($i = 0; $i < $count; $i++) {
            $values[] = ['TimeStamp' => $start + $i * $step, 'Value' => $i * 0.5];
        }
        return $values;
    }

    private function setMockStatus(int $status): void
    {
        file_put_contents(self::$mockDir . '/status', (string) $status);
    }

    private function clearRequests(): void
    {
        @unlink(self::$mockDir . '/requests.jsonl');
    }

    private function writeRequests(): array
    {
        $file = self::$mockDir . '/requests.jsonl';
        if (!file_exists($file)) {
            return [];
        }
        $requests = array_map(function ($line)
        {
            return json_decode($line, true);
        }, file($file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES));
        return array_values(array_filter($requests, function ($request)
        {
            return $request['method'] == 'POST';
        }));
    }

    /**
     * VariableID => list of ['ts' => int, 'value' => float] from successful-looking write bodies
     */
    private function writtenPoints(): array
    {
        $points = [];
        foreach ($this->writeRequests() as $request) {
            foreach (explode("\n", $request['body']) as $line) {
                if (!preg_match('/,variableid=(\d+)[ ,].*?\bvalue=([^ ]+) (\d+)$/', $line, $m)) {
                    continue;
                }
                $points[intval($m[1])][] = ['ts' => intval($m[3]), 'value' => floatval($m[2])];
            }
        }
        return $points;
    }
}
