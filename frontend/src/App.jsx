import { useState, useEffect } from 'react'
import { Routes, Route, Navigate } from 'react-router-dom'
import { Box } from '@mui/material'
import Login from './pages/Login'
import Dashboard from './pages/Dashboard'
import Scanner from './pages/Scanner'
import ProtectedRoute from './components/ProtectedRoute'
import { useAuth } from './contexts/AuthContext'

// Dashboard component imports
import DashboardAssistantWeaver from './dashboards/DashboardAssistantWeaver'
import DashboardWeavingMonitor from './dashboards/DashboardWeavingMonitor'
import DashboardBobbinWinder from './dashboards/DashboardBobbinWinder'
import DashboardCardPuncher from './dashboards/DashboardCardPuncher'
import DashboardFilatureSupplier from './dashboards/DashboardFilatureSupplier'
import DashboardGraphDrafter from './dashboards/DashboardGraphDrafter'
import DashboardLogFinishing from './dashboards/DashboardLogFinishing'
import DashboardLoomHarnessSetter from './dashboards/DashboardLoomHarnessSetter'
import DashboardMasterColorist from './dashboards/DashboardMasterColorist'
import DashboardMasterWeaver from './dashboards/DashboardMasterWeaver'
import DashboardPetniMaster from './dashboards/DashboardPetniMaster'
import DashboardPirnWinders from './dashboards/DashboardPirnWinders'
import DashboardQADyeingInspector from './dashboards/DashboardQADyeingInspector'
import DashboardQualityInspector from './dashboards/DashboardQualityInspector'
import DashboardSilkDegummingMaster from './dashboards/DashboardSilkDegummingMaster'
import DashboardSilkGrader from './dashboards/DashboardSilkGrader'
import DashboardSilkMarkOfficer from './dashboards/DashboardSilkMarkOfficer'
import DashboardSystemAdmin from './dashboards/DashboardSystemAdmin'
import DashboardSkeinDyeMaster from './dashboards/DashboardSkeinDyeMaster'
import DashboardStoreInventoryManager from './dashboards/DashboardStoreInventoryManager'
import DashboardSUPLoomFloorSupervisor from './dashboards/DashboardSUPLoomFloorSupervisor'
import DashboardThrowsterTwister from './dashboards/DashboardThrowsterTwister'
import DashboardWarpBeamPreparation from './dashboards/DashboardWarpBeamPreparation'
import DashboardWarpJoiner from './dashboards/DashboardWarpJoiner'
import DashboardZariInspector from './dashboards/DashboardZariInspector'
import DashboardSKUManager from './dashboards/DashboardSKUManager'
import DashboardSKUComparison from './dashboards/DashboardSKUComparison'
import DashboardDesignGenerator from './dashboards/DashboardDesignGenerator'
import DashboardBuyBackManager from './dashboards/DashboardBuyBackManager'
import DashboardGuildManager from './dashboards/DashboardGuildManager'
import DashboardIoTDeviceManager from './dashboards/DashboardIoTDeviceManager'
import DashboardLocalizationManager from './dashboards/DashboardLocalizationManager'

const DASHBOARD_COMPONENTS = {
  'ROLE-ASSISTANT-WEAVER': DashboardAssistantWeaver,
  'ROLE-BOBBIN-WINDER': DashboardBobbinWinder,
  'ROLE-CARD-PUNCHER': DashboardCardPuncher,
  'ROLE-FILATURE-SUPPLIER': DashboardFilatureSupplier,
  'ROLE-GRAPH-DRAFTER': DashboardGraphDrafter,
  'ROLE-LOG-FINISHING': DashboardLogFinishing,
  'ROLE-LOOM-HARNESS-SETTER': DashboardLoomHarnessSetter,
  'ROLE-MASTER-COLORIST': DashboardMasterColorist,
  'ROLE-MASTER-WEAVER': DashboardMasterWeaver,
  'ROLE-PETNI-MASTER': DashboardPetniMaster,
  'ROLE-PIRN-WINDERS': DashboardPirnWinders,
  'ROLE-QA-DYEING-INSPECTOR': DashboardQADyeingInspector,
  'ROLE-QUALITY-INSPECTOR': DashboardQualityInspector,
  'ROLE-SILK-DEGUMMING-MASTER': DashboardSilkDegummingMaster,
  'ROLE-SILK-GRADER': DashboardSilkGrader,
  'ROLE-SILK-MARK-OFFICER': DashboardSilkMarkOfficer,
  'ROLE-SYSTEM-ADMIN': DashboardSystemAdmin,
  'ROLE-SKU-MANAGER': DashboardSKUManager,
  'ROLE-SKEIN-DYE-MASTER': DashboardSkeinDyeMaster,
  'ROLE-STORE-INVENTORY-MANAGER': DashboardStoreInventoryManager,
  'ROLE-SUP-LOOM-FLOOR-SUPERVISOR': DashboardSUPLoomFloorSupervisor,
  'ROLE-THROWSTER-TWISTER': DashboardThrowsterTwister,
  'ROLE-WARP-BEAM-PREPARATION': DashboardWarpBeamPreparation,
  'ROLE-WARP-JOINER': DashboardWarpJoiner,
  'ROLE-ZARI-INSPECTOR': DashboardZariInspector,
  'ROLE-DESIGN-GENERATOR': DashboardDesignGenerator,
  'ROLE-BUY-BACK-MANAGER': DashboardBuyBackManager,
  'ROLE-GUILD-MANAGER': DashboardGuildManager,
  'ROLE-IOT-DEVICE-MANAGER': DashboardIoTDeviceManager,
  'ROLE-LOCALIZATION-MANAGER': DashboardLocalizationManager,
}

function App() {
  const { user, loading } = useAuth()

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="100vh">
        <div>Loading...</div>
      </Box>
    )
  }

  const DashboardComponent = user ? DASHBOARD_COMPONENTS[user.role?.role_id] : null

  return (
    <Routes>
      <Route path="/login" element={user ? <Navigate to="/dashboard" /> : <Login />} />
      <Route path="/dashboard" element={
        <ProtectedRoute>
          {DashboardComponent ? <DashboardComponent /> : <Dashboard />}
        </ProtectedRoute>
      } />
      <Route path="/scanner" element={
        <ProtectedRoute>
          <Scanner />
        </ProtectedRoute>
      } />
      <Route path="/sku-comparison" element={
        <ProtectedRoute>
          {user?.role?.role_id === 'ROLE-SKU-MANAGER' || user?.role?.role_id === 'ROLE-SYSTEM-ADMIN' ? <DashboardSKUComparison /> : <Navigate to="/dashboard" />}
        </ProtectedRoute>
      } />
      <Route path="/" element={<Navigate to="/dashboard" />} />
    </Routes>
  )
}

export default App
