import useSWR from "swr";

import {
  BiliType,
  fetcher,
  LiveStreamerEntity, proxy,
  User
} from "./api-streamer";
import {useEffect, useState} from "react";

const NOFACE_URL = "https://i0.hdslb.com/bfs/face/member/noface.jpg";

interface BiliUserProfile extends User {
  faceBlob: Blob | null;
}

export interface BiliUserItem extends User {
  face: string;
}

async function fetchFaceBlob(faceUrl?: string): Promise<Blob | null> {
  const primaryUrl = faceUrl || NOFACE_URL;
  try {
    const pRes = await proxy(`/bili/proxy?url=${encodeURIComponent(primaryUrl)}`);
    return await pRes.blob();
  } catch (err) {
    if (primaryUrl !== NOFACE_URL) {
      try {
        const fallbackRes = await proxy(`/bili/proxy?url=${encodeURIComponent(NOFACE_URL)}`);
        return await fallbackRes.blob();
      } catch (fallbackErr) {
        console.error(fallbackErr);
      }
    } else {
      console.error(err);
    }
    return null;
  }
}

async function fetchBiliUserProfile(item: User): Promise<BiliUserProfile> {
  try {
    const res = await fetcher(`/bili/space/myinfo?user=${encodeURIComponent(item.value)}`, undefined);
    if (!res?.data?.name) {
      throw new Error(res?.message || "Cookie已失效");
    }
    const faceBlob = await fetchFaceBlob(res.data.face);
    return {
      ...item,
      name: res.data.name,
      faceBlob,
    };
  } catch (error) {
    console.error(error);
    const faceBlob = await fetchFaceBlob(NOFACE_URL);
    return {
      ...item,
      name: "Cookie已失效",
      faceBlob,
    };
  }
}

export default function useStreamers() {
  const { data, error, isLoading } = useSWR<LiveStreamerEntity[]>("/v1/streamers", fetcher);

  return {
    isLoading,
    streamers: data,
  };
}

export function useBiliUsers() {
  const {data: users, error: usersError, isLoading: usersLoading} = useSWR<User[]>("/v1/users", fetcher);
  const profilesKey =
    users && users.length > 0
      ? ["bili-users-profiles", users.map((u) => `${u.id}:${u.value}`).join("|")]
      : null;
  const {
    data: profiles,
    error: profilesError,
    isLoading: profilesLoading,
  } = useSWR<BiliUserProfile[]>(
    profilesKey,
    () => Promise.all((users ?? []).map(fetchBiliUserProfile)),
    { revalidateOnFocus: false, dedupingInterval: 30000 }
  );

  const [list, setList] = useState<BiliUserItem[]>([]);
  useEffect(() => {
    if (!profiles || profiles.length === 0) {
      setList([]);
      return;
    }
    const createdUrls: string[] = [];
    const nextList = profiles.map(({ faceBlob, ...user }) => {
      let face = "";
      if (faceBlob) {
        face = URL.createObjectURL(faceBlob);
        createdUrls.push(face);
      }
      return {
        ...user,
        face,
      };
    });
    setList(nextList);
    return () => {
      createdUrls.forEach((url) => URL.revokeObjectURL(url));
    };
  }, [profiles]);

  return {
    isLoading: usersLoading || profilesLoading,
    isError: usersError || profilesError,
    biliUsers: list,
  };
}

export function useTypeTree() {
  const { data: archivePre, error, isLoading } = useSWR("/bili/archive/pre", fetcher);
  const treeData = archivePre?.data?.typelist.map((type: BiliType)=> {
    return {
      label: type.name,
      value: type.id,
      children: type.children
    };
  });
  return {
    isLoading,
    isError: error,
    typeTree: treeData,
  };
}

export interface Season {
  season_id: number;
  title: string;
  section_id: number;
}

export function useSeasons(userCookie: string | undefined) {
  const { data, error, isLoading } = useSWR<Season[]>(
    userCookie ? `/bili/seasons?user=${encodeURIComponent(userCookie)}` : null,
    fetcher
  );
  return {
    isLoading,
    isError: error,
    seasons: data,
  };
}
