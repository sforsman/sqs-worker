<?php

// TODO: Remove echo -> Monolog

namespace SQSWorker;

class Worker
{
  protected $queue_url;
  protected $client;
  protected $executors;
  protected $had_job;

  public function __construct(AbstractQueue $client)
  {
    $this->client = $client;
    $this->client->setCallback([$this, 'processResult']);
    echo "Worker waking up\n";
  }

  public function teach($name, $callback)
  {
    if(!is_callable($callback)) {
      throw new \Exception("Invalid callback");
    }

    if(isset($this->executors[$name])) {
      throw new \Exception("Already know how to work with {$name}");
    }

    $this->executors[$name] = $callback;
    echo "-> Learned {$name}\n";
  }

  // Main loop
  public function work()
  {
    if(!$this->executors) {
      throw new \Exception("I'd rather be educated first");
    }

    $this->had_job = true;

    // Main loop
    while($this->client->loop()) {
      try {
        if($this->had_job) {
          echo "Waiting for work\n";
          $this->had_job = false;
        }
        // Expected to block
        $this->client->receive();
      } catch (\Exception $e) {
        // Just rethrow to see what comes
        file_put_contents("php://stderr", "-> FATAL: Message receive failed, worker dying on ".get_class($e). ": ".$e->getMessage()."\n");
        exit(1);
      }
    }
  }

  public function processResult($result)
  {
    $this->had_job = true;

    if(count($result) === 0) {
      // Silently skipping (meant for supporting long poll timeouts)
      return;
    }
    // Do something with the message
    $body = $result['body'];
    $id   = $result['id'];
    $tag  = $result['tag'];

    echo "-> Picked up message: {$id}\n";

    $data = json_decode($body, true);
    if(!$data) {
      file_put_contents("php://stderr", "-> ERROR: Invalid data, json_decode failed\n");
      $this->client->nack($tag);
      return;
    }

    if(!isset($data['Function'])) {
      file_put_contents("php://stderr", "-> ERROR: Invalid data, no function defined\n");
      $this->client->nack($tag);
      continue;
    }

    if(!isset($this->executors[$data['Function']])) {
      file_put_contents("php://stderr", "-> ERROR: I don't know how to handle {$data['Function']}\n");
      $this->client->nack($tag);
      return;
    }

    $callable = $this->executors[$data['Function']];
    if(!is_callable($callable)) {
      file_put_contents("php://stderr", "-> ERROR: Function is not callable (anymore)\n");
      $this->client->nack($tag);
      return;
    }

    $parms = isset($data['Parameters']) ? $data['Parameters'] : [];

    if(!is_array($parms)) {
      file_put_contents("php://stderr", "-> ERROR: Parameters must be an array\n");
      $this->client->nack($tag);
      return;
    }

    // We don't care about the return value
    $failed = false;
    echo "-> Executing {$data['Function']}\n";
    try {
      unset($data['Function'], $data['Parameters']);
      call_user_func($callable, $parms, $data);
    } catch(\Exception $e) {
      file_put_contents("php://stderr", "-> ERROR: Executor threw an ".get_class($e). ": ".$e->getMessage()."\n");
      $failed = true;
    }

    // Currently we ack even if the executor crashes
    try {
      if($failed) {
        $this->client->nack($tag);
      } else {
        $this->client->ack($tag);
      }
    } catch(\Exception $e) {
      file_put_contents("php://stderr", "-> FATAL: Message (n)ack failed, worker dying on ".get_class($e). ": ".$e->getMessage()."\n");
      exit(2);
    }
  }
}
