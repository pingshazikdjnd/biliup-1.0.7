'use client'
import React from 'react'
import { Button, Card, Divider, Layout, Space, Tag, Typography } from '@douyinfe/semi-ui'
import { IconGlobe } from '@douyinfe/semi-icons'
import { AuthGuard } from './lib/auth-guard'
import ProtectedLayout from './lib/protected-layout'

const { Title, Text, Paragraph } = Typography

type ChangeKind = 'feat' | 'fix' | 'warn'

interface ChangeItem {
  kind: ChangeKind
  title: string
  detail: string
}

interface ReleaseEntry {
  version: string
  date: string
  latest?: boolean
  summary?: string
  items: ChangeItem[]
}

const REPO_RELEASES_URL = 'https://github.com/pingshazikdjnd/biliup-1.0.7/releases'

const RELEASES: ReleaseEntry[] = [
  {
    version: '1.0.7.009',
    date: '2026-11-25',
    latest: true,
    summary: '新增外挂 mesio 下载器支持、跨夜标题日期校准与稳定性收口。',
    items: [
      {
        kind: 'feat',
        title: '新增外挂 mesio 下载器支持（FLV/HLS 实时时间戳修复）',
        detail:
          '全局下载设置与单主播配置覆写均支持选择 mesio，并新增 mesio_path 可执行文件路径配置；采用 .mesio_tmp 隔离会话目录分段原子移出、软水位实时停机穿透 delay 与冷启动崩溃残留恢复，解决 B 站 720p 录播单 P 时间戳跳变问题（支持缺失/启动异常自动回退 ffmpeg）。',
      },
      {
        kind: 'fix',
        title: '跨夜连续录播标题日期以最早分段为准并加固 DB 反查（补丁更新）',
        detail:
          '上传前遍历 file_list 全部分段竞争全局最早开录日期（按数据库记录、文件名时间戳、真实文件 birth-time 优先级恢复，仅在全缺失时以 st_mtime 末位兜底，防止 Linux 无 birth-time 或转码 remux 乱序导致标题日期误写为次日），并阻断 fallback_meta 透传后段 date 与空 date 异常；加固 get_stream_info_by_filename（按主播过滤、兼容重名与转码 stem 匹配、异常日志提升为 WARNING），并在 ffmpeg 非零退出保留非空 .part 时补记 FileList。',
      },
      {
        kind: 'fix',
        title: '上传熔断、覆写弹窗、热重载与 Rust Server 加固',
        detail:
          '补充软水位触发标识传递与立即上传、OverrideModal 状态保持、热重载安全校验与 Rust Server 细节加固。',
      },
    ],
  },
  {
    version: '1.0.7.008',
    date: '2026-11-24',
    summary: '全栈稳定性与安全加固大版本（涵盖 PR #4 ~ PR #13 共 10 个专项修复）。',
    items: [
      {
        kind: 'feat',
        title: '可配置双水位磁盘保护',
        detail:
          '全局下载设置新增软水位（disk_upload_threshold_gb，分段完成后主动退出本轮以触发上传与清理，不中断后续录播）与硬水位（disk_stop_threshold_gb，触发告警并暂停新开录制，保住数据库与 Cookie 文件）。',
      },
      {
        kind: 'feat',
        title: 'QQ 邮箱 / SMTP 告警与上传错误智能分级',
        detail:
          '全局上传设置新增 SMTP 邮件告警（支持 465 SSL / 587 STARTTLS，30 分钟同类错误冷却防轰炸）；识别不可自愈错误（-101 未登录、Cookie 失效、风控验证码 21015/21016、参数/标签违禁等）首次即熔断，瞬态网络错误按重试上限自动重试。',
      },
      {
        kind: 'feat',
        title: 'WebUI 主页内置本分支更新日志',
        detail:
          '替换原先指向外站上游文档的 iframe，主页直接内置 Semi UI 更新日志（自适应深色模式、离线秒开），同步维护 1.0.7.001 ~ 1.0.7.008 完整变更记录。',
      },
      {
        kind: 'fix',
        title: '彻底杜绝上传失败误删录像',
        detail:
          'BiliWeb.upload 检查子进程非零退出码并可靠关闭管道；上传异常不再被吞掉，仅在上传正常返回非空列表时才执行后处理删除；修复 Webhook 告警丢失真实错误详情并补充 15 秒超时。',
      },
      {
        kind: 'fix',
        title: 'WebUI 全路径密码鉴权与代理安全加固',
        detail:
          '开启 --password 后全面保护 /api/、/url-status、/v1/、/bili/、/static/ 与 /ws/logs（放行 OPTIONS 预检并使用恒定时间密码比对）；校验已登记 Cookie 路径；/bili/proxy 限制协议、禁用重定向、超时 15 秒且限制 10MB 响应体；前端日志下载与历史回放完整适配鉴权。',
      },
      {
        kind: 'fix',
        title: '消除全局 Cookie 污染与 config.yaml 截断 Bug',
        detail:
          '健康检查与快手/虎牙插件改为按请求传头，不再污染全局 client.headers；Config.save() 改为读取旧配置后临时文件原子替换。',
      },
      {
        kind: 'fix',
        title: '并发锁安全、.part 保护与状态同步',
        detail:
          'NamedLock 增加元锁防竞态；上传锁原子标记防跨线程误释锁；录制中跳过 .part 文件防误删改名；修复无 {streamer} 自定义文件名匹配；Config.load_from_db 原子替换并支持模板 0 值覆盖。',
      },
      {
        kind: 'fix',
        title: '消除 /streamers 请求风暴与多账号串号',
        detail:
          '弹窗按需请求账号与合集信息，隔离多账号 B 站请求会话，修复合集 section_id 校验与各上传器返回契约。',
      },
      {
        kind: 'fix',
        title: '调度防重入、覆写弹窗与多平台插件修复',
        detail:
          '修复下载调度重入竞态、Windows 文件名 % 转义、OverrideModal 清空/覆写状态，并修复 Bilibili（完整保留 bili_qn=0、CDN 惩罚回退与等待转码）、虎牙、抖音、快手、TwitCasting、猫耳、克拉克拉、Nico、YouTube、斗鱼等插件问题。',
      },
      {
        kind: 'fix',
        title: '实时日志流、前端认证与运维热重载加固',
        detail:
          '/ws/logs 改为二进制按完整行增量读取并支持文件截断/轮转检测；修复日志页 Hook 顺序、UTF-8 鉴权编码、投稿管理页鉴权包裹与延迟发布 dtime 同步；全链路升级 B 站接口至 HTTPS，移除二维码轮询 eval，修复只读迁移目录与热重载安全重启，并修复 Dockerfile 构建源。',
      },
      {
        kind: 'fix',
        title: 'Rust 层加固与 Axum 0.8 适配',
        detail:
          'Cookie 刷新改为只读加载 + Unix 0600 权限原子替换与乐观防并发覆盖；扫码轮询增加 180 秒总超时；分页计算防除零/下溢；上传线路测速增加 10 秒超时与容错；PyO3 登录绑定全部释放 GIL；biliup-cli Server 适配 Axum 0.8、代理白名单防 SSRF、监控器并发安全与 SQLx 0.8 CRUD。',
      },
      {
        kind: 'warn',
        title: '升级与配置提醒',
        detail:
          '若启用了 --password，升级后请重新登录 WebUI 刷新鉴权状态；小容量磁盘用户建议前往「空间配置」按需设置软/硬磁盘水位及 QQ 邮箱/Webhook 告警。',
      },
    ],
  },
  {
    version: '1.0.7.007',
    date: '2026-07-26',
    items: [{ kind: 'fix', title: '稳定性修复', detail: '修复若干已知 Bug。' }],
  },
  {
    version: '1.0.7.006',
    date: '2026-07-26',
    items: [{ kind: 'fix', title: '问题修复', detail: '修复 issues/3 反馈的问题。' }],
  },
  {
    version: '1.0.7.005',
    date: '2026-07-25',
    items: [{ kind: 'fix', title: '合集修复', detail: '修复合集功能无法使用的 Bug。' }],
  },
  {
    version: '1.0.7.004',
    date: '2026-07-24',
    items: [{ kind: 'feat', title: '构建发布', detail: '发布 1.0.7.003 编译版本。' }],
  },
  {
    version: '1.0.7.003',
    date: '2026-07-24',
    items: [{ kind: 'feat', title: '合集下拉选择', detail: '添加合集功能，合集支持下拉自选。' }],
  },
  {
    version: '1.0.7.002',
    date: '2026-07-24',
    items: [{ kind: 'fix', title: '问题修复', detail: '修复 issues/1 反馈的问题。' }],
  },
  {
    version: '1.0.7.001',
    date: '2026-07-10',
    items: [
      {
        kind: 'feat',
        title: '主备 CDN 智能回退',
        detail:
          '主要 CDN 十分钟内断流时下次请求自动切换至回退 CDN API；若回退同样十分钟内断流则循环切回；任一 API 稳定录播超过十分钟后重置优先使用主要 CDN。',
      },
      {
        kind: 'feat',
        title: '等待转码流录制',
        detail: '新增等待转码流录制功能，开播时会等待目标清晰度出现后再开始录制。',
      },
    ],
  },
]

const KIND_META: Record<ChangeKind, { emoji: string; label: string; color: 'green' | 'blue' | 'orange' }> = {
  feat: { emoji: '💡', label: '新功能', color: 'green' },
  fix: { emoji: '🔧', label: '已修复', color: 'blue' },
  warn: { emoji: '⚠️', label: '需注意', color: 'orange' },
}

const Home: React.FC = () => (
  <AuthGuard>
    <ProtectedLayout>
      <Layout
        style={{
          height: '100%',
          overflowY: 'auto',
          backgroundColor: 'var(--semi-color-bg-0)',
        }}
      >
        <div
          style={{
            maxWidth: 960,
            width: '100%',
            margin: '0 auto',
            padding: '28px 24px 48px',
          }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'flex-start',
              flexWrap: 'wrap',
              gap: 12,
              marginBottom: 16,
            }}
          >
            <div>
              <Space align="center" spacing={10}>
                <Title heading={2} style={{ margin: 0 }}>
                  更新日志
                </Title>
                <Tag color="violet" type="light">
                  biliup-1.0.7
                </Tag>
              </Space>
              <Paragraph type="secondary" style={{ marginTop: 6, marginBottom: 0 }}>
                CHANGELOG · 当前分支定制版本功能与修复记录
              </Paragraph>
            </div>
            <Space>
              <Button
                icon={<IconGlobe />}
                theme="light"
                onClick={() => window.open(REPO_RELEASES_URL, '_blank', 'noopener,noreferrer')}
              >
                GitHub Releases
              </Button>
            </Space>
          </div>

          <Space wrap spacing={12} style={{ marginBottom: 20 }}>
            <Tag color="green" type="light">
              💡 新添加的功能
            </Tag>
            <Tag color="blue" type="light">
              🔧 已修复的问题
            </Tag>
            <Tag color="orange" type="light">
              ⚠️ 需要手动操作的更新信息
            </Tag>
          </Space>

          <Divider margin="0 0 20px 0" />

          <Space vertical align="start" spacing={16} style={{ width: '100%' }}>
            {RELEASES.map((release) => (
              <Card
                key={release.version}
                style={{ width: '100%' }}
                headerStyle={{ padding: '14px 20px' }}
                bodyStyle={{ padding: '14px 20px 18px' }}
                title={
                  <Space align="center" spacing={10} wrap>
                    <Title heading={4} style={{ margin: 0 }}>
                      {release.version}
                    </Title>
                    {release.latest && (
                      <Tag color="green" type="solid" size="small">
                        Latest
                      </Tag>
                    )}
                    <Text type="tertiary" size="small">
                      {release.date}
                    </Text>
                  </Space>
                }
                headerExtraContent={
                  <Button
                    theme="borderless"
                    size="small"
                    onClick={() =>
                      window.open(
                        `${REPO_RELEASES_URL}/tag/${encodeURIComponent(release.version)}`,
                        '_blank',
                        'noopener,noreferrer'
                      )
                    }
                  >
                    查看发布页
                  </Button>
                }
              >
                {release.summary && (
                  <Paragraph type="secondary" style={{ marginTop: 0, marginBottom: 14 }}>
                    {release.summary}
                  </Paragraph>
                )}
                <ul style={{ margin: 0, paddingLeft: 0, listStyle: 'none' }}>
                  {release.items.map((item, idx) => {
                    const meta = KIND_META[item.kind]
                    return (
                      <li
                        key={`${release.version}-${idx}`}
                        style={{
                          display: 'flex',
                          alignItems: 'flex-start',
                          gap: 10,
                          marginBottom: idx === release.items.length - 1 ? 0 : 10,
                          lineHeight: 1.65,
                        }}
                      >
                        <Tag
                          color={meta.color}
                          type="light"
                          size="small"
                          style={{ flexShrink: 0, marginTop: 2 }}
                        >
                          {meta.emoji} {meta.label}
                        </Tag>
                        <div>
                          <Text strong>{item.title}：</Text>
                          <Text>{item.detail}</Text>
                        </div>
                      </li>
                    )
                  })}
                </ul>
              </Card>
            ))}
          </Space>
        </div>
      </Layout>
    </ProtectedLayout>
  </AuthGuard>
)

export default Home
