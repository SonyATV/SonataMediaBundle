<?php
/*
 * This file is part of the Sonata project.
 *
 * (c) Thomas Rabaix <thomas.rabaix@sonata-project.org>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Sonata\MediaBundle\CDN;

use Aws\S3\S3Client;
use Aws\S3\Exception\S3Exception;

/**
 *
 * @uses S3Client for stablish connection with S3 service
 * @author Javier Spagnoletti <phansys@gmail.com>
 */
class S3 implements CDNInterface
{
    /**
     * @var string
     */
    protected $bucket;

    /**
     * @var string
     */
    protected $directory;

    /**
     * @var string
     */
    protected $key;

    /**
     * @var string
     */
    protected $secret;

    /**
     * @var integer
     */
    protected $expirationInterval;

    /**
     * @var S3Client
     */
    protected $client;

    /**
     * @param string $path
     * @param string $key
     * @param string $secret
     * @param string $distributionId
     */
    public function __construct($bucket, $directory, $key, $secret, $expirationInterval)
    {
        $this->bucket = $bucket;
        $this->directory = $directory;
        $this->key = $key;
        $this->secret = $secret;
        $this->expirationInterval = $expirationInterval;
    }

    /**
     * {@inheritdoc}
     */
    public function getPath($relativePath, $isFlushable = false)
    {
        return $this->getClient()->getObjectUrl(
            $this->bucket,
            $this->computePath($relativePath),
            !is_null($this->expirationInterval) ? time() + (int) $this->expirationInterval : null
        );
    }

    /**
     * {@inheritDoc}
     */
    public function flushByString($string)
    {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    public function flush($string)
    {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    public function flushPaths(array $paths)
    {
        // nothing to do
    }

    /**
     * {@inheritdoc}
     */
    public function getFlushStatus($identifier)
    {
        // nothing to do
    }

    /**
     * Return a S3Client
     *
     * @return S3Client
     */
    private function getClient()
    {
        if (!$this->client) {
            $this->client = S3Client::factory(array(
                'key'    => $this->key,
                'secret' => $this->secret
            ));
        }

        return $this->client;
    }

    /**
     * For testing only
     *
     * @param $client
     *
     * @return void
     */
    public function setClient($client)
    {
        $this->client = $client;
    }

    /**
     * @param string $key
     *
     * @return string
     */
    protected function computePath($key)
    {
        if (empty($this->directory)) {
            return $key;
        }

        return sprintf('%s/%s', $this->directory, $key);
    }
}
