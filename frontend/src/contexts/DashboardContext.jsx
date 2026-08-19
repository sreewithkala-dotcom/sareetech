import { createContext, useContext, useState, useEffect } from 'createContext'
import axios from 'axios'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api/v1'

const DashboardContext = createContext(null)

export function DashboardProvider({ children }) {
  const [inputQueue, setInputQueue] = useState([])
  const [notifications, setNotifications] = useState([])
  const [loading, setLoading] = useState(true)

  const refreshInputQueue = async () => {
    try {
      const response = await axios.get(`${API_URL}/dashboard/input-queue`)
      setInputQueue(response.data.lots || [])
    } catch (error) {
      console.error('Failed to fetch input queue:', error)
    } finally {
      setLoading(false)
    }
  }

  const addNotification = (message, type = 'info') => {
    const id = Date.now()
    setNotifications(prev => [...prev, { id, message, type }])
    setTimeout(() => {
      setNotifications(prev => prev.filter(n => n.id !== id))
    }, 5000)
  }

  const value = {
    inputQueue,
    notifications,
    loading,
    refreshInputQueue,
    addNotification
  }

  return (
    <DashboardContext.Provider value={value}>
      {children}
    </DashboardContext.Provider>
  )
}

export function useDashboard() {
  const context = useContext(DashboardContext)
  if (!context) {
    throw new Error('useDashboard must be used within a DashboardProvider')
  }
  return context
}
