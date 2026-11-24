"use client";

import { checkBasicAuth, useAuth } from './auth-context';
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { Spin } from '@douyinfe/semi-ui';

export function AuthGuard({ children }: { children: React.ReactNode }) {
  const { isAuthenticated } = useAuth();
  const router = useRouter();
  const [checked, setChecked] = useState(false);

  useEffect(() => {
    let active = true;
    if (!isAuthenticated) {
      const auth = localStorage.getItem('auth');
      checkBasicAuth(auth).then(ok => {
        if (!active) return;
        if (ok) {
          setChecked(true);
        } else {
          router.push('/login');
        }
      });
    } else {
      setChecked(true);
    }
    return () => {
      active = false;
    };
  }, [isAuthenticated, router]);

  // 如果用户已认证或不需要认证且已检查，显示子组件
  if (isAuthenticated || checked) {
    return <>{children}</>;
  }

  // 如果用户未认证且正在检查，显示加载状态
  if (!isAuthenticated && !checked) {
    return (
      <div style={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        height: '100vh',
      }}>
        <Spin size="large" />
      </div>
    );
  }

  // 其他情况不显示内容
  return null;
}