<?php

// The PW worker

require 'vendor/autoload.php';

use SQSWorker\Worker;
use SQSWorker\SQSQueue;
use Aws\Sqs\SqsClient;

// TODO: Add monolog stuff, then disable these
error_reporting(E_ALL);
ini_set("display_errors", true);

$client = SqsClient::factory([
  // These are fine for now, but really need to consider whether we want to
  // store them inside the container's environment. We could at least create
  // an IAM profile that has only access to SQS
  'key'    => getenv('AWS_ACCESS_KEY_ID'), 
  'secret' => getenv('AWS_SECRET_ACCESS_KEY'), 
  'region' => getenv('AWS_REGION'),
]);

// Note: Settings do not change after creation
$queue = $client->createQueue([
  'QueueName'  => 'pw-' . getenv('PW_DB_NAME'),
  'Attributes' => [
    // 'DelaySeconds'       => 5, // Default is zero
    // 'MaximumMessageSize' => 4096, // The default is 256 KB
    // 'MessageRetentionPeriod' => 60 * 60 * 24 // Default is 4 days

    // 60 seconds is more than enough for a worker to pick up the job, mark it as started and remove the message
    'VisibilityTimeout'  => 60, // Default is 30
  ],
]);

$queueUrl = $queue->get('QueueUrl');

$queue = new SQSQueue(['client'=>$client, 'queue_url'=>$queueUrl]);
$worker = new Worker($queue);

// Just a dummy job
$worker->teach("test", function($parameters = []) {
  echo "-> PW execution with parameters: " . json_encode($parameters) . "\n";
  sleep(5); // Simulate hard work
  // TODO: Acquire locks, check that the job has not been started and update so
});

// Meant for testing recovery
$worker->teach("test_kill", function($parameters = []) {
  aadfg();
});

// This is far from the actual executor, but useful for simple tests
$worker->teach("pw_simple", function($parameters = []) {
  $output = "";
  $error = "";

  $retval = proc_exec("php vendor/bin/pw_exec.php", json_encode($parameters), $output, $error);

  $haveOutput = false;
  $failed = false;

  if(strlen($output))
    echo $output;
  if(strlen($error))
    file_put_contents("php://stderr", $error);

  if($retval != 0)
    file_put_contents("php://stderr", "-> Sub process failed with exit code {$retval}\n");
  else
    echo "-> OK!\n";
});

// To peek on the env
$worker->teach("dump_env", function($parameters = []) {
  echo json_encode($_SERVER) . "\n";
});

// Enter main loop
$worker->work();

// Helper function for calling sub processes
function proc_exec($cmd,$input,&$output = "",&$error = "",$cwd = false)
{
  $spec = array(
    0 => array("pipe", "r"),
    1 => array("pipe", "w"),
    2 => array("pipe", "w"),
  );

  $process = proc_open($cmd,$spec,$pipes,$cwd);

  if(!is_resource($process))
  {
    return 254;
  }

  fwrite($pipes[0], $input);
  fclose($pipes[0]);

  $output.= stream_get_contents($pipes[1]);
  fclose($pipes[1]);

  $error.= stream_get_contents($pipes[2]);
  fclose($pipes[2]);

  $retval = proc_close($process);

  return $retval;
}
