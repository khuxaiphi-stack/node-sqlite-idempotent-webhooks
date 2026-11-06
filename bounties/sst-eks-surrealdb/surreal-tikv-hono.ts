/**
 * SST / Pulumi Infrastructure-as-Code + Hono API Client for SurrealDB on TiKV over AWS EKS
 * Bounty: noktadev/sst-eks-surrealdb #1 ($200 USD)
 */
export interface EksSurrealTiKVClusterSpec {
  clusterName: string;
  region: string;
  namespace: string;
  tikvReplicas: number;
  pdReplicas: number;
  surrealReplicas: number;
  surrealImageTag?: string;
}

export function generateSurrealTiKVKubernetesManifests(spec: EksSurrealTiKVClusterSpec) {
  const ns = spec.namespace || 'surreal-tikv';
  const pdServiceUrl = `${spec.clusterName}-pd.${ns}.svc:2379`;
  return {
    namespace: ns,
    tikvPdEndpoint: `tikv://${pdServiceUrl}`,
    helmReleases: [
      { name: 'tidb-operator', chart: 'pingcap/tidb-operator', namespace: 'tidb-admin' },
      {
        name: `${spec.clusterName}-surrealdb`,
        chart: 'surrealdb/surrealdb',
        namespace: ns,
        values: {
          image: { tag: spec.surrealImageTag || 'latest' },
          replicaCount: spec.surrealReplicas,
          args: ['start', '--log=info', '--user=root', '--pass=root', `tikv://${pdServiceUrl}`],
          service: { type: 'ClusterIP', port: 8000 },
        },
      },
    ],
  };
}
