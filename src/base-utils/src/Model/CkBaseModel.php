<?php
namespace HyperfAdmin\BaseUtils\Model;

use ClickHouseDB;
use clickhousesearch\Common\Exceptions\Conflict409Exception;
use Hyperf\DbConnection\Db;
use Hyperf\DbConnection\Model\Model;
use Hyperf\Utils\Str;
use HyperfAdmin\BaseUtils\Log;
use Tinderbox\Clickhouse\Client;
use Tinderbox\Clickhouse\Server;
use Tinderbox\Clickhouse\ServerProvider;
use Tinderbox\ClickhouseBuilder\Query\Builder;

//

class CkBaseModel
{
    protected $client;

    public $logger;

    protected $lastSql;

    protected $primaryKey = 'id';


    protected $query;

    // 模糊查询字段
    protected $fuzzy_fields = [];

    // 时间类型字段
    protected $datetime_fields = [];

    // 区间类型字段
    protected $range_fields = [];

    public $mapping;

    protected $all_query;

    public function __construct()
    {
//        $builder = container(ClientBuilderFactory::class)->create();
//        $server_info = config('es.' . $this->connection);
//        if (!$server_info) {
//            throw new \Exception(sprintf('clickhouse connection [%s] not found', $this->connection));
//        }
//        if (!$this->index) {
//            throw new \Exception(sprintf('clickhouse index is required'));
//        }
//        $this->client = $builder->setHosts([$server_info])->build();
        $this->logger = Log::get('clickhouse');
//        $this->all_query = [
//            'bool' => [
//                'must' => [
//                    ['match_all' => (object)[]],
//                ],
//                'must_not' => [],
//                'should' => [],
//            ],
//        ];


        $config = [
            'host' => env("CLICKHOUSE_HOST"),
            'port' => env("CLICKHOUSE_PORT"),
            'username' => env("CLICKHOUSE_USERNAME"),
            'password' => env("CLICKHOUSE_PASSWORD")
        ];
//        $ck = new ClickHouseDB\Client($config);
////        $ck = new Client($config);
////        $ck = container(Client::class)->get();
//        $ck->database(env("CLICKHOUSE_DATABASE"));
//        $ck->setTimeout(2);      // 1 second , support only Int value
//        $ck->setTimeout(10);       // 10 seconds
//        $ck->setConnectTimeOut(5); // 5 seconds
        
        $server = new Server($config['host'],$config['port'],env("CLICKHOUSE_DATABASE"),$config['username'],$config['password']);
        $serverProvider = (new ServerProvider())->addServer($server);
        $client = new Client($serverProvider);
        $builder = new Builder($client);

//        $builder->select('st_id');
//        $builder->from($this->table);
//        $rows = $builder->get();
//        $this->logger->info($builder->toSql());
        $this->client = $builder;
//        $this->client = $ck;
    }

//    protected $operator_map = [
//        '>=' => 'gte',
//        '>' => 'gt',
//        '=' => 'eq',
//        '<=' => 'lte',
//        '<' => 'lt',
//    ];
    public function select($where = [], $attrs = [], $origin_meta = false){
        $query = $this->where2query($where);
        $query->from($this->table);
        $this->logger->info($query->toSql());
    }
    /**
     * select options 通用搜索底层方法
     *
     * @param array          $attr
     * @param array          $extra_where
     * @param string         $name_key
     * @param string|integer $id_key
     * @param string         $logic
     * @param bool           $default_query
     *
     * @return array
     */
    public function search($attr, $extra_where = [], $name_key = 'name', $id_key = 'id', $logic = 'and', $default_query = false)
    {
        $where = [];
        $kw = request()->input('kw');
        if ($kw) {
            if (preg_match_all('/^\d+$/', $kw)) {
                $where[$id_key] = $kw;
            } elseif (preg_match_all('/^\d+?,/', $kw)) {
                $where[$id_key] = explode(',', $kw);
            } else {
                $where[$name_key] = ['like' => "%{$kw}%"];
            }
        }
        $id = request()->input('id');
        if ($id) {
            if (preg_match_all('/^\d+$/', $id)) {
                $where[$id_key] = $id;
            } elseif (preg_match_all('/^\d+?,/', $id)) {
                $where[$id_key] = explode(',', $id);
            }
        }
        if (!$default_query && !$where) {
            return [];
        }
        $where['__logic'] = $logic;
        $where = array_merge($where, $extra_where);
        $attr['limit'] = $attr['limit'] ?? 100;
        return $this->list($where, $attr)->toArray();
    }

    public function list($where, array $attr, $page, $size)
    {
        $query = $this->where2query($where);
        $query->from($this->table);
        if ($attr['select'] ?? false) {
            $selects = array_map(function ($select) {
                $select = trim($select);
                if (Str::contains($select, ' ')) {
                    return Db::connection($this->getModel()->getConnectionName())->raw($select);
                } else {
                    return $select;
                }
            }, $attr['select']);
            $query->select($selects);
        }
        if (isset($attr['order_by'])) {
            $query->orderByRaw($attr['order_by']);
        }
        if (isset($attr['group_by'])) {
            // todo groupByRaw
            $query->groupBy($attr['group_by']);
        }
        $query->limit($size,($page - 1) * $size);
        $ret = $query->get();
        $this->logger->info($query->toSql());
        return $ret ? $ret->getRows() : [];
    }

//    public function select($where = [], $attrs = [], $origin_meta = false)
//    {
//        $params = [
//            'index' => $this->index,
//            'body' => [
//                'query' => $this->where2query($where) ?: $this->all_query,
//            ],
//        ];
//        $is_scroll = false;
//        if (isset($attrs['scroll'])) {
//            $is_scroll = true;
//            $params['scroll'] = $attrs['scroll'];
//        }
//        if (isset($attrs['select']) && $attrs['select'] != '*') {
//            $params['_source'] = str_replace([' ', '`'], '', $attrs['select']);
//        }
//        if (isset($attrs['offset'])) {
//            $params['body']['from'] = $attrs['offset'];
//        }
//        if (isset($attrs['limit'])) {
//            $params['body']['size'] = $attrs['limit'];
//        }
//        if (isset($attrs['order_by'])) {
//            $order_by = str_replace(['`'], '', $attrs['order_by']);
//            $order_by = preg_replace('/ +/', ' ', $order_by);
//            $explode = explode(',', $order_by);
//            $sorts = [];
//            foreach ($explode as $item) {
//                if (Str::contains($item, ['+', '-', '*', '/'])) {
//                    preg_match('/(\w+) ([+\-*\/]) (\w+) (\w+)/', $item, $m);
//                    $sorts[] = [
//                        '_script' => [
//                            'type' => 'number',
//                            'script' => [
//                                'lang' => 'painless',
//                                'source' => "doc['{$m[1]}'].value {$m[2]} doc['{$m[3]}'].value",
//                            ],
//                            'order' => $m[4],
//                        ],
//                    ];
//                } else {
//                    [
//                        $order_by_field,
//                        $order_by_type,
//                    ] = explode(' ', trim($item));
//                    $sorts[] = [
//                        $order_by_field => ['order' => $order_by_type],
//                    ];
//                }
//            }
//            $params['body']['sort'] = $sorts;
//        }
//        try {
//            $scroll_id = $attrs['scroll_id'] ?? null;
//            if (!$scroll_id) {
//                $res = $this->client->search($params);
//            } else {
//                $res = $this->client->scroll([
//                    'scroll_id' => $scroll_id,
//                    'scroll' => $attrs['scroll'],
//                ]);
//            }
//            $nex_scroll_id = $res['_scroll_id'] ?? null;
//            $list = $res['hits']['hits'] ?? [];
//            if ($origin_meta) {
//                return $list;
//            }
//            $final = [];
//            foreach ($list as $item) {
//                $final[] = $item['_source'];
//            }
//            $this->logger->info('select success', ['params' => json_encode($params, JSON_UNESCAPED_UNICODE)]);
//            if ($is_scroll) {
//                return [$final, $nex_scroll_id];
//            }
//            return $final;
//        } catch (\Exception $e) {
//            $this->logger->error(sprintf('clickhouse index:%s select error', $this->index), [
//                'exception' => $e,
//                'params' => $params,
//            ]);
//            return [];
//        }
//    }
    /**
     * 将 通用where条件 转换为 es 查询query
     *
     * @param $where array
     *
     * @return array
     */
    public function where2query($where, $query = null)
    {
//        return $query;
        $query = $query ?? $this->client;
        if (!$where) {
            return $query;
        }
        $boolean = strtolower($where['__logic'] ?? 'and');
        unset($where['__logic']);
        foreach ($where as $key => $item) {
            if (is_numeric($key) && is_array($item)) {
                $query->where(function ($query) use ($item) {
                    return $this->where2query($item, $query);
                }, null, null, $boolean);
                continue;
            }
            if (!is_array($item)) {
                $query->where($key, '=', $item, $boolean);
                continue;
            }
            if (is_real_array($item)) {
                $query->whereIn($key, $item, $boolean);
                continue;
            }
            foreach ($item as $op => $val) {
                if ($op == 'not in' || $op == 'not_in') {
                    $query->whereNotIn($key, $val, $boolean);
                    continue;
                }
                if ($op == 'like') {
                    $query->where($key, 'like', $val, $boolean);
                    continue;
                }
                if ($op == 'between') {
                    $query->whereBetween($key, $val, $boolean);
                    continue;
                }
                if ($op == 'find_in_set') { // and or
                    $query->where(function ($q) use ($val, $key) {
                        if (!is_array($val)) {
                            $val = ['values' => $val, 'operator' => 'and'];
                        }
                        $operator = $val['operator'];
                        $method = ($operator === 'or' ? 'or' : '') . "whereRaw";
                        foreach ($val['values'] as $set_val) {
                            $q->{$method}("find_in_set({$set_val}, {$key})");
                        }
                    });
                    continue;
                }
                $query->where($key, $op, $val, $boolean);
            }
        }
        return $query;
    }


    public function selectCount($where)
    {
        $query = $this->where2query($where);
        $query->from($this->table);
        $this->logger->info( $query->getCountQuery()->get()->getRows());
        return $query->getCountQuery()->get()->count();
    }

//    public function query($query, $from = null, $size = null)
//    {
//        $res = $this->_query($query, $from, $size);
//        if (empty($res) || empty($res['hits'])) {
//            return [];
//        }
//        $rows = [];
//        foreach ($res['hits']['hits'] as $row) {
//            $new_row = [];
//            foreach ($row['_source'] as $i => $v) {
//                $new_row[$i] = $v;
//            }
//            $rows[] = $new_row;
//        }
//        return $rows;
//    }

//    public function count($query)
//    {
//        $res = $this->_query($query);
//        if (empty($res) || empty($res['hits'])) {
//            return 0;
//        }
//        return $res['hits']['total'];
//    }

//    public function _query($query, $from = null, $size = null)
//    {
//        $params = [
//            'index' => $this->index,
//            'body' => [
//                'query' => $query ?: $this->all_query,
//            ],
//        ];
//        if (!is_null($from) && !is_null($size)) {
//            $params['body']['from'] = $from;
//            $params['body']['size'] = $size;
//        }
//        return $this->client->search($params);
//    }

    public function getLastSql()
    {
        return $this->lastSql;
    }

    public function getPrimaryKey()
    {
        return $this->primaryKey;
    }

}
