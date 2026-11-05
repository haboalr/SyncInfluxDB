<?php

declare(strict_types=1);

// Minimal InfluxDB HTTP mock for tests: `php -S 127.0.0.1:PORT tests/mock/influx.php`
// Records every request to $MOCK_DIR/requests.jsonl, answers with the status in $MOCK_DIR/status (default 204).

$dir = getenv('MOCK_DIR');
$body = file_get_contents('php://input');
if (($_SERVER['HTTP_CONTENT_ENCODING'] ?? '') === 'gzip') {
    $body = gzdecode($body);
}

file_put_contents($dir . '/requests.jsonl', json_encode([
    'method' => $_SERVER['REQUEST_METHOD'],
    'path'   => parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH),
    'query'  => $_GET,
    'auth'   => $_SERVER['HTTP_AUTHORIZATION'] ?? '',
    'gzip'   => ($_SERVER['HTTP_CONTENT_ENCODING'] ?? '') === 'gzip',
    'body'   => $body
]) . "\n", FILE_APPEND | LOCK_EX);

$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
switch ($path) {
    case '/health':
        http_response_code(200);
        echo '{"status":"pass"}';
        break;
    case '/api/v2/buckets':
        http_response_code(200);
        echo json_encode(['buckets' => ($_GET['name'] ?? '') === 'symcon' ? [['name' => 'symcon']] : []]);
        break;
    case '/query':
        http_response_code(200);
        echo json_encode(['results' => [['series' => [['values' => [['_internal'], ['symcon']]]]]]]);
        break;
    default:
        $status = @file_get_contents($dir . '/status');
        $status = $status ? intval($status) : 204;
        http_response_code($status);
        if ($status >= 400) {
            echo json_encode(['code' => 'invalid', 'message' => 'mock rejected write']);
        }
}
