<?php
if (!isset($_FILES['file'])) { 
    http_response_code(400); 
    echo "no file"; 
    exit; 
}
$fn = $_FILES['file']['name'];
$tmp = $_FILES['file']['tmp_name'];
$dest = __DIR__ . "/uploads/" . basename($fn);
if (!move_uploaded_file($tmp, $dest)) { 
    http_response_code(500); 
    echo "move failed"; 
    exit; 
}
echo "ok";
?>
