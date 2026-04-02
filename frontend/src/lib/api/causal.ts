import { api } from './index';

export interface CyNode {
  data: {
    id: string;
    label: string;
    node_type: string;
    prior_belief: number;
    propagated_belief: number;
    confidence: number;
    [key: string]: unknown;
  };
}

export interface CyEdge {
  data: {
    source: string;
    target: string;
    strength: number;
    mechanism: string;
    causal_type: string;
    line_style: string;
    [key: string]: unknown;
  };
}

export interface CausalDagResponse {
  elements: {
    nodes: CyNode[];
    edges: CyEdge[];
  };
  [key: string]: unknown;
}

export const causalApi = {
  getDag(studentId: string, schoolId: string) {
    return api.get<CausalDagResponse>(`/causal/students/${studentId}/schools/${schoolId}/dag`);
  },
};
