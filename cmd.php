<?php

echo '{"procesising"}' . PHP_EOL;
echo json_encode($argv) . PHP_EOL;
foreach ([1, 2, 3, 4, 5] as $i) {
    echo sprintf('{"%s"}', $i) . PHP_EOL;
    sleep(2);
}
echo '{"finished"}' . PHP_EOL;

throw new Exception("ERRRR");
