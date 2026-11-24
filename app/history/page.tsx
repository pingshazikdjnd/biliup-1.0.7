'use client'
import { Layout, Modal, Toast, Typography } from '@douyinfe/semi-ui'
import { IconUserCardVideo, IconVideoListStroked } from '@douyinfe/semi-icons'
import { Table } from '@douyinfe/semi-ui'
import { SortOrder } from '@douyinfe/semi-ui/lib/es/table'
import useSWR from 'swr'
import { fetcher, FileList } from '@/app/lib/api-streamer'
import { useRef, useState } from 'react'
import dynamic from 'next/dynamic'
import { humDate } from '@/app/lib/utils'
import { AuthGuard } from '../lib/auth-guard'
import ProtectedLayout from '../lib/protected-layout'

const Players = dynamic(() => import('@/app/ui/Player'), {
  ssr: false,
})

export default function Home() {
  const { Header, Footer, Sider, Content } = Layout
  const { data: data, error, isLoading } = useSWR<FileList[]>('/v1/videos', fetcher)
  const { Text } = Typography
  const [videoUrl, setVideoUrl] = useState<string>()
  const requestIdRef = useRef(0)
  const columns = [
    {
      title: '标题',
      dataIndex: 'name',
      render: (text: any, record: any, index: any) => {
        return <Text strong>{text}</Text>
      },
      // onFilter: (value, record) => record.name.includes(value)
    },
    {
      title: '大小',
      dataIndex: 'size',
      render: (size: number) => `${(size / 1024 / 1024).toFixed(2)} MB`,
    },
    {
      title: '更新日期',
      dataIndex: 'updateTime',
      defaultSortOrder: 'descend' as SortOrder,
      sorter: (a: any, b: any) => (a.updateTime - b.updateTime > 0 ? 1 : -1),
      render: (time: number) => humDate(time),
    },
    {
      title: '',
      dataIndex: 'operate',
      render: (text: any, record: any, index: number) => {
        return (
          <IconUserCardVideo
            style={{ cursor: 'pointer' }}
            onClick={() => showDialog(record.name)}
          />
        )
      },
    },
  ]
  const [visible, setVisible] = useState(false)
  const showDialog = async (name: string) => {
    const reqId = ++requestIdRef.current
    const base = `${process.env.NEXT_PUBLIC_API_SERVER ?? ''}/static/${encodeURIComponent(name)}`
    const auth = typeof window !== 'undefined' ? localStorage.getItem('auth') : null
    if (!auth) {
      setVideoUrl(base)
      setVisible(true)
      return
    }
    setVideoUrl(undefined)
    setVisible(true)
    try {
      const res = await fetcher(`/v1/static-token?file=${encodeURIComponent(name)}`, undefined)
      if (requestIdRef.current !== reqId) {
        return
      }
      setVideoUrl(res?.token ? `${base}?token=${encodeURIComponent(res.token)}` : base)
    } catch (e: any) {
      if (requestIdRef.current !== reqId) {
        return
      }
      Toast.error(`获取播放授权失败: ${e?.message || '未知错误'}`)
      setVisible(false)
    }
  }
  const handleCancel = () => {
    requestIdRef.current++
    setVisible(false)
    setVideoUrl(undefined)
    console.log('Cancel button clicked')
  }
  return (
    <AuthGuard>
      <ProtectedLayout>
        <Header style={{ backgroundColor: 'var(--semi-color-bg-1)' }}>
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              padding: '0 20px',
              height: '60px',
              backgroundColor: 'var(--semi-color-bg-1)',
              boxShadow: '0 1px 2px 0 rgb(0 0 0 / 0.05)',
            }}
          >
            <div
              style={{
                backgroundColor: 'rgba(var(--semi-green-4), 1)',
                borderRadius: 'var(--semi-border-radius-large)',
                color: 'var(--semi-color-bg-0)',
                display: 'flex',
                padding: '6px',
              }}
            >
              <IconVideoListStroked size="large" />
            </div>
            <h4 style={{ marginLeft: '12px', margin: 0 }}>历史记录</h4>
          </div>
        </Header>
        <Content
          style={{
            paddingLeft: 12,
            paddingRight: 12,
            backgroundColor: 'var(--semi-color-bg-0)',
          }}
        >
          <main>
            <Table size="small" columns={columns} dataSource={data} />
          </main>
          <Modal
            visible={visible}
            onCancel={handleCancel}
            closeOnEsc={true}
            style={{ width: 'min(600px, 90vw)' }}
            size="large"
            bodyStyle={{ height: 500 }}
            footer={null}
          >
            {visible && videoUrl ? (
              <Players url={videoUrl}></Players>
            ) : null}
            <div id="mse"></div>
          </Modal>
        </Content>
      </ProtectedLayout>
    </AuthGuard>
  )
}
