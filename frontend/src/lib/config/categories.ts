import Building2 from "lucide-svelte/icons/building-2";
import Shield from "lucide-svelte/icons/shield";
import BarChart3 from "lucide-svelte/icons/bar-chart-3";
import Briefcase from "lucide-svelte/icons/briefcase";
import FileText from "lucide-svelte/icons/file-text";
import Landmark from "lucide-svelte/icons/landmark";
import DollarSign from "lucide-svelte/icons/dollar-sign";
import HardHat from "lucide-svelte/icons/hard-hat";
import Leaf from "lucide-svelte/icons/leaf";
import TrendingUp from "lucide-svelte/icons/trending-up";
import HandCoins from "lucide-svelte/icons/hand-coins";
import Scale from "lucide-svelte/icons/scale";
import Users from "lucide-svelte/icons/users";
import Globe from "lucide-svelte/icons/globe";
import Database from "lucide-svelte/icons/database";
export interface CategoryMeta {
  key: string;
  label: string;
  description: string;
  icon: any;
  color: string;
}

export const categories: CategoryMeta[] = [
  {
    key: "Census",
    label: "Census Bureau",
    description: "Demographic, economic, and business statistics",
    icon: Building2,
    color: "blue",
  },
  {
    key: "BLS",
    label: "Bureau of Labor Statistics",
    description: "Employment, wages, and compensation data",
    icon: BarChart3,
    color: "emerald",
  },
  {
    key: "SEC",
    label: "SEC / EDGAR",
    description: "Securities filings and investment adviser registrations",
    icon: Shield,
    color: "purple",
  },
  {
    key: "FINRA",
    label: "FINRA",
    description: "Broker-dealer registrations and compliance",
    icon: Scale,
    color: "indigo",
  },
  {
    key: "SBA",
    label: "Small Business Administration",
    description: "SBA loan programs and PPP data",
    icon: HandCoins,
    color: "amber",
  },
  {
    key: "DOL",
    label: "Dept. of Labor",
    description: "Employee benefit plans and ERISA filings",
    icon: Briefcase,
    color: "sky",
  },
  {
    key: "IRS",
    label: "Internal Revenue Service",
    description: "Exempt organization and tax-related data",
    icon: Landmark,
    color: "red",
  },
  {
    key: "Contracts",
    label: "Federal Contracts",
    description: "Government procurement and spending data",
    icon: FileText,
    color: "teal",
  },
  {
    key: "OSHA",
    label: "OSHA",
    description: "Workplace safety inspections and injury data",
    icon: HardHat,
    color: "orange",
  },
  {
    key: "EPA",
    label: "EPA",
    description: "Environmental compliance and enforcement",
    icon: Leaf,
    color: "green",
  },
  {
    key: "FDIC",
    label: "FDIC",
    description: "Bank and financial institution data",
    icon: DollarSign,
    color: "cyan",
  },
  {
    key: "FRED",
    label: "Federal Reserve",
    description: "Economic indicators and time series data",
    icon: TrendingUp,
    color: "violet",
  },
  {
    key: "BEA",
    label: "Bureau of Economic Analysis",
    description: "National and regional economic accounts",
    icon: Globe,
    color: "rose",
  },
  {
    key: "NCUA",
    label: "NCUA",
    description: "Credit union data and supervision",
    icon: Users,
    color: "fuchsia",
  },
  {
    key: "System",
    label: "System",
    description: "Cross-reference tables and internal data",
    icon: Database,
    color: "slate",
  },
];

export const categoryMap = new Map(categories.map((c) => [c.key, c]));

export function getCategoryMeta(key: string): CategoryMeta | undefined {
  return categoryMap.get(key);
}
