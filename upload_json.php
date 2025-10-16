<?php
$raw = file_get_contents('php://input');
$body = json_decode($raw, true);
if (!$body || !isset($body['filename']) || !isset($body['data'])) { 
    http_response_code(400); 
    echo "bad json"; 
    exit; 
}
$dest = __DIR__ . "/uploads/" . basename($body['filename']);
$data = base64_decode($body['data'], true);
if ($data === false) { 
    http_response_code(400); 
    echo "bad base64"; 
    exit; 
}
if (file_put_contents($dest, $data) === false) { 
    http_response_code(500); 
    echo "write failed"; 
    exit; 
}
echo "ok";
?>
